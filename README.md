# Healthcare Analytics

<img align="center" src="https://github.com/Bayunova28/Healthcare_Analytics/blob/main/healthcare_cover.jpg" height="500" width="1000">
<p align="justify">This synthetic healthcare dataset has been created to serve as a valuable resource for data science, machine learning, and data analysis enthusiasts. It is designed to mimic real-world healthcare data, enabling users to practice, develop, and showcase their data manipulation and analysis skills in the context of the healthcare industry.<p>

## Load Dataset
```python
# Import library
import pyspark
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("Healthcare").getOrCreate()

# Load dataset
df = spark.read.load("/content/healthcare_dataset.csv",format="csv", sep=",", inferSchema="true", header="true")
df.show(10)
```
| Name              | Age | Gender | Blood Type | Medical Condition | Date of Admission | Doctor            | Hospital                 | Insurance Provider | Billing Amount | Room Number | Admission Type | Discharge Date | Medication  | Test Results  |
|-------------------|-----|--------|------------|--------------------|--------------------|-------------------|--------------------------|--------------------|----------------|-------------|----------------|----------------|-------------|---------------|
| Bobby Jackson     | 30  | Male   | B-         | Cancer             | 2024-01-31         | Matthew Smith     | Sons and Miller          | Blue Cross         | 18856.28       | 328         | Urgent         | 2024-02-02     | Paracetamol  | Normal        |
| Leslie Terry      | 62  | Male   | A+         | Obesity            | 2019-08-20         | Samantha Davies   | Kim Inc                  | Medicare           | 33643.33       | 265         | Emergency      | 2019-08-26     | Ibuprofen    | Inconclusive  |
| Danny Smith       | 76  | Female | A-         | Obesity            | 2022-09-22         | Tiffany Mitchell  | Cook PLC                 | Aetna              | 27955.10       | 205         | Emergency      | 2022-10-07     | Aspirin      | Normal        |
| Andrew Watts      | 28  | Female | O+         | Diabetes           | 2020-11-18         | Kevin Wells       | Hernandez Rogers         | Medicare           | 37909.78       | 450         | Elective       | 2020-12-18     | Ibuprofen    | Abnormal      |
| Adrienne Bell     | 43  | Female | AB+        | Cancer             | 2022-09-19         | Kathleen Hanna    | White-White              | Aetna              | 14238.32       | 458         | Urgent         | 2022-10-09     | Penicillin   | Abnormal      |
| Emily Johnson     | 36  | Male   | A+         | Asthma             | 2023-12-20         | Taylor Newton     | Nunez-Humphrey           | UnitedHealthcare   | 48145.11       | 389         | Urgent         | 2023-12-24     | Ibuprofen    | Normal        |
| Edward Edwards    | 21  | Female | AB-        | Diabetes           | 2020-11-03         | Kelly Olson       | Group Middleton          | Medicare           | 19580.87       | 389         | Emergency      | 2020-11-15     | Paracetamol  | Inconclusive  |
| Christina Martinez| 20  | Female | A+         | Cancer             | 2021-12-28         | Suzanne Thomas    | Powell Robinson          | Cigna              | 45820.46       | 277         | Emergency      | 2022-01-07     | Paracetamol  | Inconclusive  |
| Jasmine Aguilar   | 82  | Male   | AB+        | Asthma             | 2020-07-01         | Daniel Ferguson   | Sons Rich and            | Cigna              | 50119.22       | 316         | Elective       | 2020-07-14     | Aspirin      | Abnormal      |
| Christopher Berg  | 58  | Female | AB-        | Cancer             | 2021-05-23         | Heather Day       | Padilla-Walker           | UnitedHealthcare   | 19784.63       | 249         | Elective       | 2021-06-22     | Paracetamol  | Inconclusive  |

## How many avg. billing by discharge year & test results
```python
# How many avg. billing by discharge year & test results
billing_discharge_year_test = spark.sql("""
  SELECT
    YEAR(`Discharge Date`) AS `Discharge Year`,
    `Test Results`,
    ROUND(AVG(`Billing Amount`),2) AS `Avg. Billing`
  FROM Healthcare
  GROUP BY YEAR(`Discharge Date`), `Test Results`
  ORDER BY YEAR(`Discharge Date`);
""")
# Display the result
billing_discharge_year_test.show()

# Convert Spark DataFrame to Pandas DataFrame
billing_discharge_year_test_pd = billing_discharge_year_test.toPandas()
# Visualize Total Sales by Order Year & Product Category
billing_discharge_year_test_pd.groupby(['Discharge Year', 'Test Results'])['Avg. Billing'].mean().unstack().plot(marker = '.', cmap = 'viridis',
                                                                                                        figsize = (11,4))
plt.title('Avg. Billing by Discharge Year & Test Results', loc = 'center')
plt.xlabel('Discharge Year')
plt.ylabel('Avg. Billing')
plt.grid(color = 'darkgray', linestyle = ':', linewidth = 0.5)
labels, locations = plt.yticks()
plt.yticks(labels, (labels/10).astype(int))
plt.show()
```
<div align="center">

| Discharge Year | Test Results   | Avg. Billing |
|----------------|----------------|--------------|
| 2019           | Abnormal       | 25982.14     |
| 2019           | Normal         | 25900.65     |
| 2019           | Inconclusive   | 25389.02     |
| 2020           | Normal         | 25527.27     |
| 2020           | Abnormal       | 24999.46     |
| 2020           | Inconclusive   | 25472.13     |
| 2021           | Inconclusive   | 25833.06     |
| 2021           | Abnormal       | 25789.48     |
| 2021           | Normal         | 25449.27     |
| 2022           | Inconclusive   | 25713.45     |
| 2022           | Abnormal       | 25667.41     |
| 2022           | Normal         | 25202.89     |
| 2023           | Inconclusive   | 25736.98     |
| 2023           | Abnormal       | 25580.31     |
| 2023           | Normal         | 25312.44     |
| 2024           | Abnormal       | 25149.97     |
| 2024           | Normal         | 25599.08     |
| 2024           | Inconclusive   | 25350.01     |

</div>                                            
<img align="center" src="https://github.com/Bayunova28/Healthcare_Analytics/blob/main/discharge%20year%20%26%20test%20results.png" height="500" width="1100">

## How many avg. billing by test result
```python
# How many avg. billing by test result
billing_test = spark.sql("""
  SELECT
    `Test Results`,
    ROUND(AVG(`Billing Amount`),2) AS `Avg. Billing`
  FROM Healthcare
  GROUP BY `Test Results`
  ORDER BY `Avg. Billing` DESC;
""")
# Display the result
billing_test.show()

# Convert Spark DataFrame to Pandas DataFrame
billing_test_pd = billing_test.toPandas()

# Plotting the pie chart
plt.figure(figsize=(5, 4))
plt.pie(
    billing_test_pd['Avg. Billing'],
    labels=billing_test_pd['Test Results'],
    autopct='%1.1f%%',
    colors=['lightgreen', 'lightblue', 'coral'],  # Add more colors if necessary
    startangle=140
)
plt.title('Avg. Billing by Test Results')
plt.show()
```
<div align="center">

| Test Results   | Avg. Billing |
|----------------|--------------|
| Inconclusive   | 25623.69     |
| Abnormal       | 25538.35     |
| Normal         | 25456.65     |

</div>  
<div align="center">
<img align="center" src="https://github.com/Bayunova28/Healthcare_Analytics/blob/main/pie%20chart.png" height="500" width="600">
</div>  

## Inspiration
<p align='justify'>The inspiration behind this dataset is rooted in the need for practical and diverse healthcare data for educational and research purposes. Healthcare data is often sensitive and subject to privacy regulations, making it challenging to access for learning and experimentation. To address this gap, I have leveraged Python's Faker library to generate a dataset that mirrors the structure and attributes commonly found in healthcare records. By providing this synthetic data, I hope to foster innovation, learning, and knowledge sharing in the healthcare analytics domain.</p>
