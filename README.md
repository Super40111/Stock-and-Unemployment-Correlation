# Stock-and-Unemployment-Correlation

## The Project

This data pipeline uses Apache Airflow and the World Bank's APIs to collect, transform, analyze, and visualize global stock market and unemployment data. This project demonstrates utilizes data engineering, workflow automation, API integration, statistical analysis, and automatic scheduling and reporting capabilities to complete this task. This system is designed to generate automated reports, and create an interactive HTML dashboard on a weekly schedule. The data here is primarily useful for those who are interested in viewing global trends related to stocks or unemployment, such as financial analysts, economic researchers, or government agencies. 

<img width="1680" height="890" alt="Stock-Unemployment Example" src="https://github.com/user-attachments/assets/22e0d1f5-8874-4876-8d35-e5f2d885f6b5" />


The attached file is a DAG that is designed to be run in Apache Airflow. When run, it will take data from the World Bank Group regarding unemployment rates and Stock values using its API. It will then output a set csv files as well as a HTML file in into a new folder named "Data" within the Apache Airflow directory. Assuming Apache Airflow is running, the file automatically runs weekly and will attempt up to two retries spaced 5 minutes apart if there are any errors.

## Installlation

In order to run this file, you need to have [Apache Airflow](https://github.com/apache/airflow) installed. When installed, simply put the file within the "dags" folder. While this file utilizes a "data folder within the same directory, the DAG will automatically create said folder when run. All other files on this page are not necessary for the program to run and are examples of the output. 

### Side Note

The main DAG file may show a set of errors about certain library imports not being able to be resolved. These are fine as Apache Airflow has these librarys built in and will be able to handle the imports. If the file is unable to pull from the database, it will raise a ValueError.

## Output

Each of the csv files generated presents its data with each country in the x axis and each year as the y axis. Some country/year combinations do not have data associated with them and are labeled "N/A". The csv files generated are:

  - stock_raw.csv: Shows the raw stock values for each combination of country/date.
  - stock_na_removed.csv: Shows the stock values for each combination of country/date, except any countries with only "N/A" values are removed.
  - stock_year-over-year.csv: Shows the change in value for stock values compared to the previous year.
  - unemployment_raw.csv: Shows the raw unemployment values for each combination of country/date.
  - unemployment_na_removed.csv: Shows the unemployment values for each combination of country/date, except any countries with only "N/A" values are removed.
  - unemployment_year-over-year.csv: Shows the change in value for stock values compared to the previous year.
  - unemployment_stock_correlation.csv: Shows the correlation between stock values and unemployment rates (Positive correlation means when umemployment rates increase, the stock values increase).

The sole html file generated is named "correlation_chart.html". It displays a visual infographic based on the correlation between the stock value and unemployment rates of each country. The infographic also provides an average correlation alongside the ability to filter the countries shown based on how many entries each country has. Examples of these files have been provided.

Lastly, this is only a comparison between the two data points among the many different types that the World Bank tracks. The file can be easily modified to compare between separate data points. All that would need to be changed is one/both of indicator code near the top of the file, plus renaming any files/HTML text for clarity.

