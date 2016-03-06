from urlparse import urljoin
from time import sleep
import re
from tqdm import tqdm
from retrying import retry
import lxml.html
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException


ALL_SCHOOLS_PAGE = 'http://www.sports-reference.com/cbb/schools/'


gamelog_links = set()


school_name_map = {}


html = lxml.html.parse(ALL_SCHOOLS_PAGE)
# This will find all links on the page that refer to schools.
# The page has one link to itself as "/cbb/schools/" which we ignore.
school_relative_links = (
    set(html.xpath("//td[text() = '2016']/parent::tr//a/@href"))
    .difference(['/cbb/schools/']))
school_absolute_links = [urljoin(ALL_SCHOOLS_PAGE, url)
                         for url in school_relative_links]

gamelog_links = []
for url in school_absolute_links:
    for year in xrange(2014, 2016+1):
        gamelog_links.append(url + '{}-gamelogs.html'.format(year))


@retry(stop_max_attempt_number=3, wait_fixed=30*1000)
def scrape_gamelog(driver, gamelog_link):
    match = re.search('([a-z-]+)/(\d{4})-gamelogs.html', gamelog_link)
    team = match.group(1)
    year = match.group(2)
    driver.get(gamelog_link)
    sleep(1)
    html = lxml.html.fromstring(driver.page_source)
    for school_link in html.xpath(
            "//table[@id='stats_basic']//a[contains(@href, '/cbb/schools/')]"):
        school_name_map[school_link.text.strip()] = (
            re.search('/cbb/schools/([a-z-]+)/', school_link.attrib['href'])
            .group(1))
    (driver
        .find_element_by_xpath("//span[contains(@onclick, 'table2csv')]")
        .click())
    sleep(1)
    csv = driver.find_element_by_xpath('//pre').text
    with open('data/{}{}.csv'.format(team, year), 'w') as f:
        f.write(csv)


# The gamelog page has a button that will turn the stats into CSV.
# We need Javasript support for this, so we'll use a web driver.
driver_profile = webdriver.FirefoxProfile()
driver_profile.set_preference('webdriver.load.strategy', 'unstable')
driver = webdriver.Firefox(driver_profile)
driver.implicitly_wait(10)
for gamelog_link in tqdm(gamelog_links):
    try:
        scrape_gamelog(driver, gamelog_link)
    except NoSuchElementException:
        pass
driver.close()
with open('school_name_map.txt', 'w') as f:
    f.writelines('{} {}\n'.format(v, k) for k, v in school_name_map.iteritems())
