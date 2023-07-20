from selenium import webdriver
from selenium.webdriver.common.by import By
import util.constants as constants
import os
import glob
import asyncio
import functools
from humanfriendly import parse_size


@functools.total_ordering
class Downloadable:
    def __init__(self, priority, item):
        self.priority = priority
        self.item = item
    

    def __eq__(self, other) -> bool:
        return self.priority == other.priority
    

    def __lt__(self, other) -> bool:
        return self.priority < other.priority


def login(driver : webdriver.Chrome):
    login_button = driver.find_elements(By.XPATH, "//*[contains(text(), 'iniciar sessão')]")[0] # find login button
    login_button.click()
    driver.implicitly_wait(10)
    text = driver.find_elements(By.CLASS_NAME, "newlogindialog_TextInput_2eKVn")[0] # find username field
    text.send_keys(constants.USERNAME)
    text = driver.find_elements(By.CLASS_NAME, "newlogindialog_TextInput_2eKVn")[1] # find password field
    text.send_keys(constants.PASSWORD)
    btn = driver.find_elements(By.CLASS_NAME, "newlogindialog_SubmitButton_2QgFE")[0] # find send login form button
    btn.click()
    print("> Logged into account.")
    items = driver.find_elements(By.CLASS_NAME, "collectionItem")
    while len(items) == 0:
        items = driver.find_elements(By.CLASS_NAME, "collectionItem")
    print("> Authentication passed.")


def wait_for_collection_page_to_load(driver : webdriver.Chrome):
    items = driver.find_elements(By.CLASS_NAME, "collectionItem")
    while len(items) == 0:
        items = driver.find_elements(By.CLASS_NAME, "collectionItem")        
    return items


def downloadable_category(driver : webdriver.Chrome, item_id : int):   
    element_description = None
    cat = None
    try:
        element_description = driver.find_element(By.CSS_SELECTOR, "div.rightDetailsBlock a").text #one possible place the category is written
    except:
        element_description = driver.find_element(By.CSS_SELECTOR, "div.workshopTagsTitle a").text #the other possible place
    # print(f">_debug: {element_description=}")
    if (element_description is not None and element_description in constants.CATEGORIES):
        cat = element_description
    else:
        print(f"> Exception: Could not identify category. Skipping...\n | {element_description=}\n")
    if (driver.find_element(By.CLASS_NAME, "incompatibleNotification").is_displayed()):
        print(f"> Exception: {item_id} is flagged as incompatible. Skipping...")
        cat = None
    return cat


def downloadable_size(driver : webdriver.Chrome):
    size = 0
    try:
        size = driver.find_element(By.CSS_SELECTOR, "div.detailsStatRight").text
        assert "MB" in size
        size = parse_size(size)
    except:
        size = 50
    return size


def enrich_downloadable(driver : webdriver.Chrome, item : str):
    item_id = item.get_attribute("id").split("_")[1]
    item_page = f'https://steamcommunity.com/sharedfiles/filedetails/?id={item_id}'
    driver.execute_script("window.open('');")
    driver.switch_to.window(driver.window_handles[1])
    driver.get(item_page)

    data = {}
    data["id"] = item_id
    data["category"] = downloadable_category(driver, item_id)
    data["size"] = downloadable_size(driver)

    driver.close()
    driver.switch_to.window(driver.window_handles[0])

    if None in data.values():
        return Downloadable(100, {'id' : item_id, 'category' : 'undefined'})

    print(f'+ Adding item {data["id"]} to download queue. Category = {data["category"]} | Size = {data["size"]} Bytes')
    data["destination_folder"] = constants.DESTINATION_FOLDER[data["category"]]
    return Downloadable(1/data["size"], data)
    

def collect(collections, queue : asyncio.PriorityQueue):
    print("> Cleaning up environment.")
    if (os.path.isdir(constants.DEPOTCACHE)):
        manifest_files = glob.glob(os.path.join(constants.DEPOTCACHE, "*.manifest"))
        for file in manifest_files:
            os.remove(file)
        print("- Cleared depot cache.")
    if (os.path.isfile(constants.WORKCACHE)):
        os.remove(constants.WORKCACHE)
        print("- Cleared workshop cache.")
    if (collections.endswith(".txt")):
        pages = []
        lines = None
        with open(collections, "r") as c:
            lines = c.readlines()
        for line in lines:
            line = line.split("#")[0]
            if line == '':
                continue
            pages.append(line.rstrip().lstrip())
        collections = pages
        print(f"> Download {len(collections)} collections")
    else:
        collections = [collections]

    options = webdriver.ChromeOptions() 
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    driver = webdriver.Chrome(options=options)
    
    url = collections[0]
    driver.get(url)
    assert "Steam" in driver.title
    login(driver)

    count = 0
    for url in collections:
        driver.get(url)

        try:
            items = wait_for_collection_page_to_load(driver)
        except:
            print("!! Are you sure this is a collection page? Something went wrong. Skipping..")
            continue

        for item in items:
            downloadable_item = enrich_downloadable(driver, item)
            queue.put_nowait(downloadable_item)
            count += 1
    
    driver.close()
    print(f'[DEBUG] put_{count=}')
    return count