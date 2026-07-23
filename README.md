# Stock-and-Unemployment-Correlation

## The Goal

The goal of this project was to develop an automated data pipeline using Apache Airflow and the World Bank's APIs to collect, transform, analyze, and visualize global stock market and unemployment data. This system is designed to generate automated reports, and create an interactive HTML dashboard on a weekly schedule. Overall, the main purpose of this project is to demonstrate data engineering, workflow automation, API integration, statistical analysis, and reporting capabilities.

The attached file is a DAG that is designed to be run in Apache Airflow. When run, it will take data from the World Bank Group regarding unemployment rates and Stock values using its API. It will then output a set csv files as well as a HTML file in into a new folder named "Data" within the Apache Airflow directory. Assuming Apache Airflow is running, the file automatically runs weekly and will attempt up to two retries spaced 5 minutes apart if there are any errors.

## Installlation

In order to run this file, you need to have [Apache Airflow](https://github.com/apache/airflow) installed. When installed, simply put the file within the "dags" folder. While this file utilizes a "data folder within the same directory, the DAG will automatically create said folder when run. All other files on this page are not necessary for the program to run and are examples of the output.

### Side Note

The main DAG file may show a set of errors about certain library imports not being able to be resolved. These are fine as Apache Airflow has these librarys built in and will be able to handle the imports.

## Output

Each of the csv files generated presents its data with each country in the x axis and each year as the y axis. Some country/year combinations do not have data associated with them and are labeled "N/A". The csv files generated are:

  - stock_raw.csv: Shows the raw stock values for each combination of country/date.
  - stock_na_removed.csv: Shows the stock values for each combination of country/date, except any countries with only "N/A" values are removed.
  - stock_year-over-year.csv: Shows the change in value for stock values compared to the previous year.
  - unemployment_raw.csv: Shows the raw unemployment values for each combination of country/date.
  - unemployment_na_removed.csv: Shows the unemployment values for each combination of country/date, except any countries with only "N/A" values are removed.
  - unemployment_year-over-year.csv: Shows the change in value for stock values compared to the previous year.
  - unemployment_stock_correlation.csv: Shows the correlation between stock values and unemployment rates (Positive correlation means when umemployment rates increase, the stock values increase).

The sole html file generated is named "correlation_chart.html". It displays an infographic based on the correlation between the stock value and unemployment rates of each country. The infographic also provides an average correlation alongside the ability to filter the countries shown based on how many entries each country has. Examples of these files have been provided.
