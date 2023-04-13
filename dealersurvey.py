import os, sys
import datetime as dt

from celery import shared_task

# ## PROD SETTINGS
from django.conf import settings

# ## DEV SETTINGS
# main_folder = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
# sys.path.append(main_folder)
# sys.path.append(os.path.join(main_folder, 'msap'))
# import settings_LOCAL as settings

## global variables
SENDING_EMAIL_ADDRESS = "technical.team@volvo.com"
CC_EMAIL_ADDRESS = "technical.team@volvo.com"
DEVELOPER_EMAIL = "gasparo.piero.farina@volvo.com"
MAIL_SERVER_AML = "mailgot.it.volvo.net"

## SMTP protocol libraries
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage


@shared_task
def fetch_dealer_survey_data_file():
    ## specific libraries for this function
    from exchangelib import FileAttachment

    ## This function can be standardized and put in the main settings.py
    def outlook_connection():
        from exchangelib import Configuration, DELEGATE, Account
        from exchangelib import OAuth2Credentials, Identity
        from exchangelib.protocol import BaseProtocol
        import requests, requests.adapters

        ## define the proxy adapter in case you cannot reach the outlook server without.
        class ProxyAdapter(requests.adapters.HTTPAdapter):
            def send(self, *args, **kwargs):
                kwargs["proxies"] = {
                    "http": str(os.getenv("HTTP_PROXY")),
                    "https": str(os.getenv("HTTPS_PROXY")),
                }
                print("HTTP_PROXY: {}".format(os.getenv("HTTP_PROXY")))
                print("HTTPS_PROXY: {}".format(os.getenv("HTTPS_PROXY")))
                return super().send(*args, **kwargs)

        class ProxyAdapterCleaner(requests.adapters.HTTPAdapter):
            def send(self, *args, **kwargs):
                kwargs["proxies"] = {
                    "http": "",
                    "https": "",
                }
                return super().send(*args, **kwargs)

        server = "outlook.office365.com"
        email_account = "api.msap@volvo.com"
        client_id = str(os.getenv("AZURE_APP_ID"))
        client_secret = str(os.getenv("AZURE_APP_SECRET"))
        tenant_id = str(os.getenv("AZURE_TENANT_ID"))

        credentials = OAuth2Credentials(
            client_id=client_id,
            client_secret=client_secret,
            tenant_id=tenant_id,
            identity=Identity(primary_smtp_address=email_account),
        )

        config = Configuration(server=server, credentials=credentials)

        counter_try_catch = 0
        while counter_try_catch < 2:
            try:
                print(
                    "[TRY]: Establish connection to {} without proxy..".format(server)
                )
                BaseProtocol.HTTP_ADAPTER_CLS = ProxyAdapterCleaner
                my_account = Account(
                    primary_smtp_address=email_account,
                    credentials=credentials,
                    config=config,
                    autodiscover=False,
                    access_type=DELEGATE,
                )
                print("Connected to {} .".format(server))
                counter_try_catch = 2
                return my_account
            except:
                try:
                    print(
                        "[TRY]: Establish connection to {} with proxy..".format(server)
                    )
                    BaseProtocol.HTTP_ADAPTER_CLS = ProxyAdapter
                    my_account = Account(
                        primary_smtp_address=email_account,
                        credentials=credentials,
                        config=config,
                        autodiscover=False,
                        access_type=DELEGATE,
                    )
                    print("Connected to {} .".format(server))
                    counter_try_catch = 2
                    return my_account
                except:
                    print("Attempt #{} failed..".format(counter_try_catch + 1))
                    counter_try_catch += 1

        print("Connection failed for {}.".format(server))
        return False

    my_account = outlook_connection()

    counter_try_catch = 0
    while counter_try_catch < 2:
        try:
            ## init now
            now = dt.datetime.strptime(
                str(dt.datetime.now()).split(".")[0], "%Y-%m-%d %H:%M:%S"
            )

            ## fetch the name of the latest excel downloaded:
            query = """ SELECT TOP 1 ID, TRIM([FILE_NAME]) as _fileName FROM CVMS.msap.LOG_AUTO_DEALER_SURVEY_RUN ORDER BY ID desc """
            dealersurvey_log = settings.CALL_CVMS("GET", query)
            dealersurvey_log = dealersurvey_log.reset_index()
            previous_run_files = dealersurvey_log["_fileName"][0]
            last_id = int(dealersurvey_log["ID"][0])

            print("Trying to fetch the excel...")
            last_mails = my_account.inbox.filter(
                subject__startswith="TCSS Satisfaction Survey"
            ).order_by("-datetime_received")[:1]
            for mail in last_mails:
                mail_subject = mail.subject
                attachment_attributes = mail.attachments
                mail_subject = mail_subject.encode("utf8")
                mail_subject = mail_subject.upper() if mail_subject else mail_subject

                print("found:", mail_subject)

                final_file_name = ""
                for attachment in attachment_attributes:
                    print("attachment found:")
                    print(attachment.name)
                    # Check if file name was not run yet and if it is the correct type
                    if attachment.name not in previous_run_files and attachment.name[
                        -4:
                    ].lower() in [".xls", "xlsx"]:
                        if isinstance(attachment, FileAttachment):
                            ## check if the directory exists. In case not, create it.
                            local_directory = os.path.join(
                                settings.MEDIA_DIR, "auto_dealer_survey_working_dir"
                            )
                            os.makedirs(local_directory) if not os.path.isdir(
                                local_directory
                            ) else print("path found")

                            local_path = os.path.join(local_directory, attachment.name)
                            with open(local_path, "wb") as f:
                                f.write(attachment.content)
                            final_file_name = attachment.name

                        # initialized the log of this run
                        query = """
                        INSERT INTO CVMS.msap.LOG_AUTO_DEALER_SURVEY_RUN
                        VALUES ('{run_start}', {run_end}, '{user}', '{qty}', '{_fileName}', '{status}', '{success}', {outcome})
                        """.format(
                            run_start=now,
                            run_end="NULL",
                            user="MSAP",
                            qty=0,
                            _fileName=final_file_name,
                            status=1,
                            success="N",
                            outcome="NULL",
                        )
                        settings.CALL_CVMS("POST", query)

                        # send surveys
                        send_auto_dealer_survey_mail.delay(last_id + 1, final_file_name)

                    else:
                        print("File has already been run or has wrong file type")

            # if i am here, the above part runned, so there is no need to repeat other times:
            counter_try_catch = 2
        except:
            print("..Failed to fetch the excel!")
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(exc_type, exc_obj, exc_tb.tb_lineno)
            counter_try_catch += 1

    return True


@shared_task
def send_auto_dealer_survey_mail(run_id, filename):
    print("send_auto_dealer_survey_mail CALLED")
    import pandas as pd

    """Function to send automatically generated emails to workshops
        for a dealer survey. """

    def extract_data(filename):
        print("[Called]: extract_data()")
        df_ds_data = pd.read_excel(
            os.path.join(settings.MEDIA_DIR, "auto_dealer_survey_working_dir", filename)
        )

        ## safety measure for development purpose
        if settings.DEVELOP_MODE == True:
            print("Fetching only the top 3 of the excel, to not overload the system.")
            df_ds_data = df_ds_data.head(3)

        # Keep only necessary columns
        df_ds_data = df_ds_data[
            [
                "SR Number",
                "Cust-Rep Dealer Country",
                "Email",
                "Argus Company",
                "Cust-Rep Dealer Id",
                "Description Complaint",
                "App Service Display Val",
                "Cust-Rep Dealer Name",
                "Language",
            ]
        ]

        ## Due to Ukrain-Russia conflict, there is a request of suspending Dealer Surveys for Russia (02/03/2022)
        ## UPDATE (2022-03-07) Also Belarus is included in the suspension list
        try:
            cond_exclusion = df_ds_data["Cust-Rep Dealer Id"].str[:2] == "RU"
            df_ds_data = df_ds_data[~cond_exclusion]
            print("Russia has been disabled")
            cond_exclusion = df_ds_data["Cust-Rep Dealer Id"].str[:2] == "BY"
            df_ds_data = df_ds_data[~cond_exclusion]
            print("Belarus has been disabled")
        except:
            print("Disabling .. Action Failed")

        return df_ds_data

    def check_existing_columns(df_ds_data):
        print("[Called]: check_existing_columns()")
        # Needed columns: Language,Dealer_country
        MIN_COLUMNS_DEALER_SURVEY_DATA = [
            "Cust-Rep Dealer Country",
            "Cust-Rep Dealer Id",
            "SR Number",
            "Email",
            "Description Complaint",
            "Argus Company",
            "App Service Display Val",
            "Cust-Rep Dealer Name",
            "Language",
        ]

        column_ok_counter = 0
        column_names = list(df_ds_data.columns)
        for item in column_names:
            if item in MIN_COLUMNS_DEALER_SURVEY_DATA:
                column_ok_counter += 1

        if column_ok_counter == len(MIN_COLUMNS_DEALER_SURVEY_DATA):
            return True
        else:
            return False

    def data_check(df_ds_data):
        print("[Called]: data_check()")
        # Check for missing data
        try:
            df_rows_missing_data = df_ds_data[df_ds_data.isnull().any(axis="columns")]
        except:
            print("failed to check missing data in excel.")
            df_rows_missing_data = pd.DataFrame()

        # Correct rows
        df_correct_ds_data = df_ds_data.dropna()

        ## CORRECT VALUES FOR INSERT INTO DB
        # Escape the quote..
        try:
            df_correct_ds_data["Cust-Rep Dealer Name"] = df_correct_ds_data[
                "Cust-Rep Dealer Name"
            ].str.replace("'", "''")
        except:
            df_correct_ds_data["Cust-Rep Dealer Name"] = df_correct_ds_data[
                "Cust-Rep Dealer Name"
            ]

        return df_correct_ds_data, df_rows_missing_data

    def check_for_doubles(df_correct_ds_data):
        print("[Called]: check_for_doubles()")
        # Load argus cases already launched in the last 4 months (from SAP BO the report is already filtered on the cases opened in the last 3 months)
        # previous script was looking at the entire population, but it's a useless load on the system, which grows with the time..
        query = """ 
        SELECT ARGUS_NUMBER as argus_number FROM CVMS.msap.LOG_DEALER_SURVEY_COMMUNICATION
        WHERE DATEDIFF(DAY, DATE_SEND, GETDATE()) <= 120
        """
        existing_communication = settings.CALL_CVMS("GET", query)

        # accomodate for first run
        if not existing_communication.empty:
            # Join with presumed new data
            DF_merg = pd.merge(
                df_correct_ds_data,
                existing_communication,
                left_on="SR Number",
                right_on="argus_number",
                how="left",
            )

            # Filter argus nr that are unique
            df_correct_ds_data = DF_merg[DF_merg["argus_number"].isnull()]
            df_doubles = DF_merg[DF_merg["argus_number"].notnull()]
        else:
            df_doubles = []
        return df_doubles, df_correct_ds_data

    def send_dealer_survey_mail(df_correct_ds_data):
        print("[Called]: send_dealer_survey_mail()")
        ## init now
        now = dt.datetime.strptime(
            str(dt.datetime.now()).split(".")[0], "%Y-%m-%d %H:%M:%S"
        )

        for i, row in df_correct_ds_data.iterrows():
            ## the external try-catch to ensure that if a case fails, we can keep going with the remaining cases..
            try:
                # initialize the object for each email to be send.
                query = """
                INSERT INTO CVMS.msap.LOG_DEALER_SURVEY_COMMUNICATION
                VALUES ('{date_send}', '{argus_number}', '{dealer_name}', '{country}', '{brand}', '{fully_processed}', '{service}', '{dealer_nr}')
                """.format(
                    date_send=now,
                    argus_number=row["SR Number"],
                    dealer_name=row["Cust-Rep Dealer Name"],
                    country=row["Cust-Rep Dealer Country"],
                    brand=row["Argus Company"],
                    fully_processed="N",  # initialize with "N". Will be update to "Y" if success, else no action.
                    service=row["App Service Display Val"],
                    dealer_nr=row["Cust-Rep Dealer Id"],
                )
                settings.CALL_CVMS("POST", query)

                # fetch the latest ID. This will be used for the update of the status.
                query = """ SELECT MAX(ID) as max_ID FROM CVMS.msap.LOG_DEALER_SURVEY_COMMUNICATION """
                max_id = settings.CALL_CVMS("GET", query)
                max_id = max_id.reset_index()
                max_id = int(max_id["max_ID"][0])

                # Check if market exception is present. Exception means that the "CC_EMAIL" has to be added to the global CC_EMAIL_ADDRESS
                cleaned_cc_addresses = []  # init
                ## fetch exceptions table
                query = """ SELECT * FROM CVMS.msap.DEALER_SURVEY_MARKET_EXCEPTIONS WHERE LANGUAGE = '{language}' AND MARKET = '{market}'""".format(
                    language=row["Language"], market=row["Cust-Rep Dealer Country"]
                )
                exceptions = settings.CALL_CVMS("GET", query)
                exceptions = exceptions.reset_index()
                if not exceptions.empty:
                    ## Check if the Brand of this exception is either "*" or same value as row["Argus Company"]
                    cond = (exceptions["BRAND"][0] == "*") | (
                        exceptions["BRAND"][0] == row["Argus Company"]
                    )
                    if not cond:
                        cc_addresses = exceptions["CC_EMAIL"][0].split("/")
                        cleaned_cc_addresses = list(map(str.strip, cc_addresses))
                cleaned_cc_addresses.append(CC_EMAIL_ADDRESS)

                try:
                    email_destination_list = []
                    # use predetermined adress of MARKET_INFO table only
                    email_destination_list.append(row["Email"])
                    # Select the sender of the emaila ccording to the brand
                    email_sender = SENDING_EMAIL_ADDRESS
                    # Select reply to adresses
                    reply_to_adresses = [SENDING_EMAIL_ADDRESS]
                    msg = MIMEMultipart("related")

                    # Create mail
                    msg["From"] = email_sender
                    msg["To"] = ", ".join(email_destination_list)
                    msg["Cc"] = ", ".join(cleaned_cc_addresses)
                    msg["reply-to"] = ", ".join(reply_to_adresses)
                    msg["Subject"] = "Customer Satisfaction Survey: {}".format(
                        row["SR Number"]
                    )
                    mail = smtplib.SMTP(MAIL_SERVER_AML, 25, timeout=20)

                    # Look for correct template
                    location = os.path.join(
                        settings.STATIC_DIR, "mail_templates", "dealersurvey"
                    )
                    email_code = open(
                        os.path.join(location, "look_and_feel.html"),
                        "r",
                        encoding="utf-8-sig",
                    )
                    email_look_and_feel = email_code.read()
                    ## If the correct template is not found, the survey is sent in ENGLISH (default)
                    try:
                        email_code = open(
                            os.path.join(location, "{}_dealer_survey.html").format(
                                row["Language"]
                            ),
                            "r",
                            encoding="utf-8-sig",
                        )
                    except:
                        email_code = open(
                            os.path.join(location, "ENG_dealer_survey.html"),
                            "r",
                            encoding="utf-8-sig",
                        )
                    email_text = email_code.read()

                    # Add dynamic data
                    email_text = email_text.format(
                        row["SR Number"], row["Description Complaint"]
                    )

                    final_email_code = email_look_and_feel + email_text

                    # Create full mail html code
                    part1 = MIMEText(final_email_code, "html")
                    msg.attach(part1)

                    # Open and attach TDS logo
                    fp = open(
                        os.path.join(
                            settings.STATIC_DIR,
                            "mail_templates",
                            "dealersurvey",
                            "logo.jpg",
                        ),
                        "rb",
                    )
                    msg_image = MIMEImage(fp.read())
                    fp.close()
                    msg_image.add_header("Content-ID", "<image1>")
                    msg.attach(msg_image)

                    # Sending the mail to 'customer'
                    # Check if in develop mode to avoid mails being send to customer.
                    if settings.DEVELOP_MODE == False:
                        print("Sending Mail to Dealer. Develop Mode OFF")
                        # Make a combination of to addresses and cc addresses
                        all_receivers = email_destination_list + cleaned_cc_addresses
                        # Send the mail
                        mail.sendmail(email_sender, all_receivers, msg.as_string())
                        # Log automail case as ok
                        query = """ UPDATE CVMS.msap.LOG_DEALER_SURVEY_COMMUNICATION SET FULLY_PROCESSED = '{status}' WHERE ID = '{id}' """.format(
                            status="Y", id=max_id
                        )
                        settings.CALL_CVMS("POST", query)
                    else:
                        print("Develop Mode ON. Mail goes to the developer.")
                        # Mail will be send in debug but to previously selected developer email adresses
                        mail.sendmail(email_sender, DEVELOPER_EMAIL, msg.as_string())
                        # Log automail case as ok
                        query = """ UPDATE CVMS.msap.LOG_DEALER_SURVEY_COMMUNICATION SET FULLY_PROCESSED = '{status}' WHERE ID = '{id}' """.format(
                            status="Q", id=max_id
                        )
                        settings.CALL_CVMS("POST", query)

                    # Cleanly quit mail object
                    mail.quit()

                except Exception as e:
                    # In case of problems log automail case as NOK
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(exc_type, fname, exc_tb.tb_lineno)
                    print(e)
                    # Log for failure
                    query = """ UPDATE CVMS.msap.LOG_DEALER_SURVEY_COMMUNICATION SET FULLY_PROCESSED = '{status}' WHERE ID = '{id}' """.format(
                        status="Y", id=max_id
                    )
                    settings.CALL_CVMS("POST", query)
            except Exception as e:
                # In case of problems log automail case as NOK
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)
                print(e)
                ## this will be a case to be revisited, but at least we know which are failing and why..
                query = """
                INSERT INTO CVMS.msap.LOG_DEALER_SURVEY_COMMUNICATION
                VALUES ('{date_send}', '{argus_number}', '{dealer_name}', '{country}', '{brand}', '{fully_processed}', '{service}', '{dealer_nr}')
                """.format(
                    date_send=now,
                    argus_number=row["SR Number"],
                    dealer_name="Unknow",
                    country="Unknow",
                    brand="Unknow",
                    fully_processed="N",
                    service="Unknow",
                    dealer_nr="Unknow",
                )
                settings.CALL_CVMS("POST", query)

    def main():
        outcome_string = {"A": 1, "B": 1, "C": 1}
        df_ds_data = extract_data(filename)
        try:
            ## init now
            now = dt.datetime.strptime(
                str(dt.datetime.now()).split(".")[0], "%Y-%m-%d %H:%M:%S"
            )
            if check_existing_columns(df_ds_data):
                query = """
                UPDATE CVMS.msap.LOG_AUTO_DEALER_SURVEY_RUN
                SET 
                RUN_END = '{run_end}', 
                STATUS = '{status}'
                WHERE ID = '{run_id}'
                """.format(
                    run_end=now, status=2, run_id=run_id
                )
                settings.CALL_CVMS("POST", query)

                ## This function STORES two csv. They are connected to what you can download from the MSAP portal.
                df_correct_ds_data, df_rows_missing_data = data_check(df_ds_data)
                if len(df_rows_missing_data) > 0:
                    outcome_string["B"] = 0
                    csv_title = "{}_dealersurvey_missing_data.csv".format(run_id)
                    ## check if the directory exists. In case not, create it.
                    local_directory = os.path.join(
                        settings.MEDIA_DIR, "downloadables", "dealer_survey"
                    )
                    os.makedirs(local_directory) if not os.path.isdir(
                        local_directory
                    ) else print("path found")
                    ## save the file in the directory
                    full_result_path = os.path.join(local_directory, csv_title)
                    df_rows_missing_data.to_csv(full_result_path, sep=";", index=False)

                df_doubles, df_correct_ds_data = check_for_doubles(df_correct_ds_data)
                if len(df_doubles) > 0:
                    outcome_string["C"] = 0
                    ## check if the directory exists. In case not, create it.
                    local_directory = os.path.join(
                        settings.MEDIA_DIR, "downloadables", "dealer_survey"
                    )
                    os.makedirs(local_directory) if not os.path.isdir(
                        local_directory
                    ) else print("path found")
                    ## save the file in the directory
                    csv_title = "{}_dealersurvey_double_data.csv".format(run_id)
                    final_file_name_doubles = os.path.join(local_directory, csv_title)

                    df_doubles.to_csv(final_file_name_doubles, sep=";", index=False)

                ## cast outcome_string variable in the proper way
                outcome_string = str(outcome_string)
                outcome_string = outcome_string.replace("'", '"')

                query = """
                UPDATE CVMS.msap.LOG_AUTO_DEALER_SURVEY_RUN
                SET 
                RUN_END = '{run_end}', 
                STATUS = '{status}'
                WHERE ID = '{run_id}'
                """.format(
                    run_end=now, status=3, run_id=run_id
                )
                settings.CALL_CVMS("POST", query)
                send_dealer_survey_mail(df_correct_ds_data)

                query = """
                UPDATE CVMS.msap.LOG_AUTO_DEALER_SURVEY_RUN
                SET 
                RUN_END = '{run_end}',
                QTY_SURVEYS = '{qty}', 
                STATUS = '{status}',
                SUCCES = '{success}',
                OUTCOME = '{outcome}'
                WHERE ID = '{run_id}'
                """.format(
                    run_end=now,
                    qty=len(df_correct_ds_data),
                    status=5,
                    success="Y",
                    outcome=outcome_string,
                    run_id=run_id,
                )
                print(query)
                settings.CALL_CVMS("POST", query)

            else:
                # When necessary columns not fount
                outcome_string["A"] = 0
                query = """
                UPDATE CVMS.msap.LOG_AUTO_DEALER_SURVEY_RUN
                SET 
                RUN_END = '{run_end}',
                QTY_SURVEYS = '{qty}', 
                STATUS = '{status}',
                SUCCES = '{success}',
                OUTCOME = '{outcome}'
                WHERE ID = '{run_id}'
                """.format(
                    run_end=now,
                    qty=len(df_correct_ds_data),
                    status=5,
                    success="N",
                    outcome=outcome_string,
                    run_id=run_id,
                )
                settings.CALL_CVMS("POST", query)
        except Exception as e:
            # In case of problems log automail case as NOK
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print("-" * 10, exc_type, fname, exc_tb.tb_lineno)
            print(e)

        print("send_auto_dealer_survey_mail FINISHED")

    main()


# send_auto_dealer_survey_mail(run_id=179, filename="TCSS Satisfaction Survey-20230403.XLSX")
