
import asyncio
from pyppeteer import launch
import time
import  pandas as pd
import numpy as np
import matplotlib.pyplot as plt

DEALS_URL="https://spacerace.filecoin.io/deals/"
#Change to your minerid
MINER_ID=["t01277","t01291"]

TOTAL_PAGE= 11
GROUP_FREQUENCY= '12H'

async def asyncmain(url):
    browser = await launch()
    page = await browser.newPage()
    await page.goto(url)
    #Hard-code for now, but should use waitForSelector
    # await page.waitForSelector('td.retrieve')
    time.sleep(3)
    page_nbr = "page_" + str(int((int)(url.split("=")[-1])/50)) + ".png"
    await page.screenshot({'path': page_nbr, 'fullPage': True})
    table = await page.querySelectorEval('table', '(element) => element.outerHTML')
    await browser.close()
    return table

def web2df():
    frames = []
    for from_url in get_urls():
        table = asyncio.get_event_loop().run_until_complete(asyncmain(from_url))
        df = pd.read_html(table)[0]
        #Change column "Created" to datetime
        df['Created'] = pd.to_datetime(df['Created'])
        #Use column "Created" as index
        df.set_index("Created",inplace=False)
        frames.append(df)
    return pd.concat(frames)

def get_urls():
    deals_arr = []
    miner_join = "%20".join(MINER_ID)
    for i in range(0,TOTAL_PAGE):
        deal_page = DEALS_URL + miner_join + "?skip=" + str(50*i)
        deals_arr.append(deal_page)
    return deals_arr

def get_retrieval_data(df,filter='retrieve',group_freq=GROUP_FREQUENCY):
    df_filter = df[df["Type"] == filter]
    df_filter["Success"] = (df_filter["Message"] == "success")
    df_gb = df_filter.groupby([pd.Grouper(key='Created',freq=group_freq),df_filter.Success]).size().reset_index(name='Count')
    df_success_count = df_gb[df_gb["Success"]== True]
    df_fail_count = df_gb[df_gb["Success"]== False]
    pd.concat({
        'RetrieveOK': df_success_count.set_index('Created').Count,
        'RetrieveFail': df_fail_count.set_index('Created').Count
    }, axis=1).plot.bar()
    plt.savefig('RetrieveData.png',bbox_inches='tight')
def main():
    df = web2df()
    get_retrieval_data(df)

if __name__ == '__main__':
    main()