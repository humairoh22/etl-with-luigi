import pandas as pd
import numpy as np
import luigi
import time
import requests
from pangres import upsert
from datetime import datetime
from bs4 import BeautifulSoup as bs
from helper.db_connector import source_engine, dw_engine
from helper.validator import validation_process

class ExtractSalesData(luigi.Task):
    def requires(self):
        pass

    def run(self):
        #init process engine
        db_source = source_engine()

        #query for fetch data from table
        query = "select * from amazon_sales_data"

        #read data from database using pandas
        df = pd.read_sql(query, db_source)

        #save output to csv file
        df.to_csv(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget("data/extract/data_sales.csv")

class ExtractDataProduct(luigi.Task):
    def requires(self):
        pass

    def run(self):

        #get the link of data source 
        url = "https://docs.google.com/spreadsheets/d/1UNzNWcP5R5ZGkZUnJXGlqvurxC9XOwIDDsy3Jy-KWMY/edit?gid=1925085368#gid=1925085368"
        link = url.replace("edit?gid=1925085368#", "export?format=csv&")

        #read data from the link given using pandas
        df = pd.read_csv(link)

        df.to_csv(self.output().path, index=False)

    def output(self):

        return luigi.LocalTarget("data/extract/data_product.csv")     

class ScrapingData(luigi.Task) :

    def output(self):

        return luigi.LocalTarget("data/extract/data_scrapped.csv")

    def run(self):

        extract_news = []

        #scrape news from page 1 to 15 using beautifulsoup
        for page in range(1, 16):
            try:
                resp = requests.get(f"https://www.jpnn.com/teknologi/?page={page}")

                soup = bs(resp.content, "html.parser")
                articles = soup.find_all("div", class_ = "content-description")

                for article in articles:
                    find_h2 = article.find("h2")
                    find_h6 = article.find("h6")

                    #extract link, title, and news created in tag h2
                    if find_h2:
                        get_link = find_h2.find("a").get("href")
                        title = find_h2.text.strip()
                    
                    if find_h6:
                        categories = find_h6.text.strip()
                    else:
                        continue

                    news_created = article.find("span", class_ = "silver")

                    if news_created:
                        date_article = news_created.text.strip()

                    else:
                        "Date not found"
                    
                    date_scrapped = datetime.now().strftime("%m/%d/%Y %H:%M:%S")

                    get_data = {
                        "URL":get_link,
                        "title":title,
                        "categories":categories,
                        "news_created":date_article,
                        "scrapped_at": date_scrapped
                    }

                    extract_news.append(get_data)
                    
                    #set time to delay when extract each news to avoid as a bot
                    time.sleep(0.1)
            
            except Exception as e:
                print(f"Error when parcing the article: {e}")

        df_scrapped = pd.DataFrame(extract_news)
        df_scrapped.to_csv(self.output().path, index=False)
    
class DataValidation(luigi.Task):

    def requires(self):
        return [ExtractSalesData(), ExtractDataProduct(),
                ScrapingData()]

    def run(self):
        
        validate_sales_data = pd.read_csv(self.input()[0].path)
        validate_product_data = pd.read_csv(self.input()[1].path)
        validate_scrapped_data = pd.read_csv(self.input()[2].path)

        #validate data after extraction
        validation_process(data=validate_sales_data, table_name="sales")
        validation_process(data=validate_product_data, table_name="product")
        validation_process(data=validate_scrapped_data, table_name="scrapped_data")

    def output(self):
        pass

class TransformDataSales(luigi.Task):
    def requires(self):
        return ExtractSalesData()
    
    def run(self):
        df_sales = pd.read_csv(self.input().path)

        #drop unused column and duplicates rows 
        df_sales = df_sales.drop('Unnamed: 0', axis = 1)
        df_sales = df_sales.drop_duplicates()

        #create function to remove non number and convert it to numeric
        def convert_to_numeric(value):
            cleaned_value = pd.to_numeric(value.str.replace(r'[^0-9.]', '', regex=True),
                                  errors='coerce')
            return cleaned_value
        
        #apply function for cleaning some columns
        df_sales['actual_price'] = convert_to_numeric(df_sales['actual_price'])
        df_sales['discount_price'] = convert_to_numeric(df_sales['discount_price'])
        df_sales['ratings'] = convert_to_numeric(df_sales['ratings'])

        df_sales.to_csv(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget("data/transform/data_sales_transformed.csv")

class TransformDataProduct(luigi.Task):
    def requires(self):
        return ExtractDataProduct()
    
    def run(self):
        df_product = pd.read_csv(self.input().path)
        df_product = df_product.drop(df_product.filter(regex="Unnamed").columns, axis=1)

        #rename columns
        RENAME_COLS = {'prices.amountMax':'amountmax', 'prices.amountMin':'amountmin', 'prices.availability':'availability', 'prices.condition':'condition',
            'prices.currency':'currency', 'prices.dateSeen':'dateseen', 'prices.isSale':'issale','prices.merchant':'merchant', 'prices.shipping':'shipping',
            'prices.sourceURLs':'sourceurl', 'dateAdded':'dateadded', 'dateUpdate':'dateaupdate', 'primaryCategories':'primarycategories',
            'manufacturerNumber':'manufacturenumber'}

        df_product = df_product.rename(columns=RENAME_COLS)

        df_product['condition'] = df_product['condition'].replace({'new':'New', 'Seller refurbished':'Refurbished', 'pre-owned':'Used', 'Manufacturer refurbished':'Refurbished', 'refurbished':'Refurbished',
                                                        'New other (see details)':'New'})

        df_product['availability'] = df_product['availability'].replace({'Yes':'In Stock', 'TRUE':'In Stock', 'yes':'In Stock', '32 available':'In Stock', '7 available':'In Stock',
                                                               'undifined':'Out of Stock', 'No':'Out of Stock', 'More on the Way':'Out of Stock', 'sold':'Out of Stock',
                                                               'FALSE':'Out of Stock'})
        df_product.to_csv(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget("data/transform/data_product_transformed.csv")
    
class TransformDataScraping(luigi.Task):

    def requires(self):
        return ScrapingData()
    
    def run(self):
        df_scrape = pd.read_csv(self.input().path)

        #remove duplicates values if any
        df_scrape = df_scrape.drop_duplicates()
        df_scrape['categories'] = df_scrape['categories'].str.split(' ').str[0]
        df_scrape['news_created'] = df_scrape['news_created'].str.split(',').str[1]
        df_scrape['news_created'] = df_scrape['news_created'].replace({' – ':' '}, regex=True)

        df_scrape.to_csv(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget("data/transform/data_scraped_transformed.csv")
    
class LoadData(luigi.Task):
    def requires(self):
        return [TransformDataSales(),
        TransformDataProduct(),
        TransformDataScraping()]

    def run(self):

        dwh_engine = dw_engine()

        table_name = ["sales", "products", "articlescrape"]

        for idx, tb_name in enumerate(table_name):

            data = pd.read_csv(self.input()[idx].path)

            data.insert(0, "no_id", range(0, 0 + len(data)))

            data = data.set_index("no_id")

            #for insert or update data
            upsert(con = dwh_engine, 
                    df = data,
                    table_name = tb_name,
                    if_row_exists = "update")

if __name__ == "__main__":
    luigi.build([ExtractSalesData(),
                 ExtractDataProduct(),
                 ScrapingData(),
                 DataValidation(),
                 TransformDataSales(),
                 TransformDataProduct(),
                 TransformDataScraping(),
                 LoadData()], local_scheduler=True)
