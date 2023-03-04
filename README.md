# DBT Marketing Data Engineering Project

This project aims to provide a clean and structured database for marketing data that can be easily queried and analyzed. It leverages the data modeling and transformation tool, DBT (Data Build Tool), to build a scalable and maintainable data pipeline.

## Project Structure

. <br>
├── dbt_project.yml     # DBT project configuration file <br>
├── models              # Directory to store DBT models <br>
│   ├── raw             # Directory to store base models <br>
│   ├── transormations  # Directory to store transformation models <br>
|   ├── reporting       # Directory to store reporting models <br>
│   └── README.md       # Documentation for models <br>
├── README.md           # Project documentation <br>
└── schema.yml          # YAML file to define database schema <br>


* **dbt_project.yml:** This file is the configuration file for the DBT project. It includes project-level settings such as project name, version, and required dependencies.

* **models:** This directory contains the DBT models that transform and load the data. It is divided into three subdirectories: **raw**, **transformation** and **reporting**. The raw directory contains the raw models that define the database schema, such as tables and views. The transformaiton directory contains the models that transform and load the marketing data into Google Big Query. The reporting models then contains the models that are build on above of transformation models.

* **README.md:** This file contains the documentation for the entire project.

* **schema.yml:** This file defines the database schema. It specifies the tables, columns, and their data types.


## How to Use
To use this project, follow these steps:

1. Clone this repository to your local machine.
2. Install DBT by following the instructions in the DBT documentation.
3. Update the dbt_project.yml file to reflect your project name and dependencies.
4. Update the schema.yml file to define your database schema.
5. Get you data from source to raw models.
6. Update the models in the martech directory to transform and load your specific marketing data.
7. Run DBT to build and populate the database.

## Contributing
If you would like to contribute to this project, please follow these steps:

1. Fork this repository.
2. Create a new branch with your changes: git checkout -b my-new-branch.
3. Make your changes and commit them: git commit -am 'Add some feature'.
4. Push to the branch: git push origin my-new-branch.
5. Create a pull request.


# Data Visualization: 

After transforming Facebook data using DBT Cloud, data visualization can be an effective way to gain insights and communicate the results to stakeholders. With a clean and structured database, it becomes easier to create visualizations that highlight trends, patterns, and anomalies in the data. By using tools like Tableau, Power BI, or Python libraries such as Matplotlib or Seaborn, you can create interactive and dynamic visualizations that enable users to explore the data in different ways. Data visualization can also help to identify areas for improvement, such as targeting underperforming campaigns or identifying new opportunities for growth. Overall, data visualization is a crucial component of any data engineering project, as it helps to bridge the gap between technical and non-technical stakeholders, and enables better decision-making based on data-driven insights.

Here are some sample dashboards by using PowerBI:

## Facebook:

![image](https://user-images.githubusercontent.com/27678469/222919960-eda9974a-06a2-40e1-bdbc-c30606f63e96.png)

<br>
<br>

![image](https://user-images.githubusercontent.com/27678469/222919980-f1444382-556d-4658-9de9-e2ecc292cc22.png)

## Google Ads:

![image](https://user-images.githubusercontent.com/27678469/222920033-356ab9c6-fee9-4425-9529-c8be69d7b92c.png)

<br>
<br>

## Combined Data: 

![image](https://user-images.githubusercontent.com/27678469/222920074-3a5b3c3e-4704-4298-a3be-13603b39cb86.png)








