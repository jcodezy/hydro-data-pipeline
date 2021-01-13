import os 
from pathlib import Path
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
from time import sleep
from datetime import datetime, timedelta  
from selenium import webdriver 
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.select import Select
from selenium.webdriver.common.keys import Keys

def download_yesterdays_csv_raw():

    """
    This function uses Selenium to log into my hydro service provider, navigate to the data export centre 
    and downloads a csv file of a yesterday's usage by the hour. 
    """

    try: 
        # variables 
        EMAIL=os.getenv('EMAIL2')
        PASSWORD=os.getenv('BC_HYDRO_PASSWORD')
        DRIVER_PATH = os.getenv('CHROME_DRIVER_PATH')
        DOWNLOAD_PATH = os.getenv('BC_HYDRO_DOWNLOAD_PATH')

        # instantiate webdriver and preferences 
        chrome_options = webdriver.ChromeOptions()
        prefs = {"download.default_directory": DOWNLOAD_PATH} 
        chrome_options.add_experimental_option("prefs", prefs)
        driver = webdriver.Chrome(executable_path=DRIVER_PATH, options=chrome_options)  

        driver.get('https://www.bchydro.com/index.html')
        driver.find_element_by_xpath(""" //div[@id='bchh-loginblock']/button[@id='btnLogin'] """).click()

        # Find username & password fields on login page
        driver.find_element_by_xpath(""" //input[@id='email'] """).send_keys(EMAIL)
        sleep(1)
        driver.find_element_by_xpath(""" //input[@id='password'] """).send_keys(PASSWORD)
        sleep(1)
        driver.find_element_by_xpath(""" //div[@class='formButtons-wrapper']/input[@name='btnSubmit'] """).click()

        # hover over 'shortcuts' menu;
        sleep(1)
        hover_menu = driver.find_element_by_xpath(""" //div[@class='acctsrv_shortcuts'] """)
        ActionChains(driver).move_to_element(hover_menu).perform()

        # then click 'data export centre'4  
        driver.find_element_by_xpath(""" //div[@class='extra']/ul/li/a[@class='icon billing_export'] """).click()
        sleep(1)
        driver.execute_script("window.scrollTo(0, 500)")
        driver.find_element_by_id("""data-export-consumption-history""").click()
        sleep(1) 
        driver.find_element_by_id("""export-format-csv""").click()

        # get from_date and to_date objects into proper formats 
        from_date =  datetime.date(datetime.now()) - timedelta(days=2) # day before yesterday 
        from_date_day = from_date.strftime("%d") 
        from_date_month = from_date.strftime("%b") # 'Jan', 'Feb', etc  
        from_date_year = from_date.strftime("%Y")

        to_date = datetime.date(datetime.now()) - timedelta(days=1)
        to_date_day = to_date.strftime("%d")
        to_date_month = to_date.strftime("%b")
        to_date_year = to_date.strftime("%Y")
        sleep(1)

        #  Fill out date range box #1 (from date)
        driver.find_element_by_xpath(""" //input[@id='fromdate-datepicker'] """).click()
        sleep(1)
        select = Select(driver.find_element_by_xpath(""" //select[@class='ui-datepicker-month'] """))
        sleep(1)
        select.select_by_visible_text(from_date_month)
        sleep(1)
        select = Select(driver.find_element_by_xpath(""" //select[@class='ui-datepicker-year'] """))
        select.select_by_visible_text(from_date_year)
        sleep(1)
        if from_date_day[0] == '0': 
            from_date_day = from_date_day[1]
            driver.find_element_by_xpath(f""" //a[@class='ui-state-default' and text()='{from_date_day}'] """).click()
        else:
            driver.find_element_by_xpath(f""" //a[@class='ui-state-default' and text()='{from_date_day}'] """).click()
        sleep(1)

        # Fill out date range box #2 (to date) 
        driver.find_element_by_xpath(""" //input[@id='todate-datepicker'] """).click()
        sleep(1)
        select = Select(driver.find_element_by_xpath(""" //select[@class='ui-datepicker-month'] """))
        select.select_by_visible_text(to_date_month)
        sleep(1)
        select = Select(driver.find_element_by_xpath(""" //select[@class='ui-datepicker-year'] """))
        select.select_by_visible_text(to_date_year)
        sleep(1)
        if to_date_day[0] == '0':
            to_date_day = to_date_day[1]
            driver.find_element_by_xpath(f""" //a[@class='ui-state-default' and text()='{to_date_day}'] """).click()
        else:
            driver.find_element_by_xpath(f""" //a[@class='ui-state-default' and text()='{to_date_day}'] """).click()
        sleep(1)

        # Select 'Hourly' interval for file download
        driver.find_element_by_xpath(""" //div[@class='chosen-container chosen-container-single chosen-container-single-nosearch'] """).click()
        sleep(1) 
        driver.find_element_by_xpath(""" //li[text()='Hourly'] """).click()
        sleep(1) 

        # Click export 
        driver.find_element_by_xpath(""" //input[@id='btnExportData'] """).click()
        sleep(5) # give some time to download 

        # Scroll up then click log out button before closing down
        driver.find_element_by_tag_name('body').send_keys(Keys.CONTROL + Keys.HOME)
        sleep(1)
        driver.find_element_by_xpath(""" //button[@id='btnLogout'] """).click()
        sleep(2)
        driver.quit()
        print("Ran successfully")

    except:
        print("Error: Did not download a csv file")
        driver.quit()