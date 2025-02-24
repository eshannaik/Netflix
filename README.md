Netflix Project -

Tech Stake used - 
 - Azure Data Factory
 - Databricks
 - PySpark
 - Azure Data Lake
 - Apache Spark
 
Source consists of 5 excel sheets namely -
- Cast
- Category
- Directors
- Countries
- Title

The project follows a medallion architecture as the data model for the project

Source to our data lake -
- Use Azure data factory to pull the data from GitHub(source), the source files are all csv files. This is stored in our Data Lake in the raw folder. We also capture the meta data from the source and store it in an string variable. 
- Then we use an Autoloader using Data Bricks to do an incremental load from our raw file into the bronze layer of our data model.
- We then move the data from our bronze layer to our silver layer, where we will do multiple transformations on the tables, and also use visualizations in the databrick notebook, to better understand our data
- Finally we will use a delta live table to move data from our silver layer to our gold layer. Using delta live tables we are able to add quality checks for our data before we move the values into the gold layer.
- All this is put into our workflow


Azure Workflow -
![image](https://github.com/user-attachments/assets/4b9c410d-c1ef-4542-8cb7-88a172a90034)

The Master Workflow -
![image](https://github.com/user-attachments/assets/89e7e890-9021-4ae6-94f9-b0cabc8fbdb4)

The entire workflow is -
![image](https://github.com/user-attachments/assets/d9f62a0e-65b6-49d4-a6de-7c73eebfe51e)
