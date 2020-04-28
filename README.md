# Pyspark
MapReduce.py 
- some pyspark map-reduce sample. 

pysparkML.py 
- k-mean classifer
*implemented k-mean to classify the movies category(k=10) by user-item matrix*
1.	Process input data from itemusermat.
2.  The first number in the line is the movie id
2.	Create Dataframe [“id”, “features”]
3.	Train k-means model
4.	Predict the data

- recommendation system
1.	Processing input data from ratings.dat
2.	Split the data
3.	Train the ALS by four different ranks [8, 10, 12, 15]
4.	Calculate MSE

