# MovieLens

MapReduce programs in Java to analyze Movie Ratings data provided by MovieLens.

GroupLens Research has collected and made available rating data sets from the MovieLens web site (http://movielens.org). The data sets were collected over various periods of time, depending on the size of the set.

I have used http://files.grouplens.org/datasets/movielens/ml-latest-small.zip for my purpose. 

I performed my analysis on the following files- 
(a) movies.csv which has movieId,title,genres
(b) ratings.csv which has userId,movieId,rating,timestamp


1.To get count of movies in all Genres - 
hadoop jar /home/cloudera/Desktop/ML_Driver.jar movielens.ML_Driver /user/cloudera/input/ml-latest-small/movies   /user/cloudera/output/movielens/genre_wise_counts

2.To get Average rating for each Genre - 
hadoop jar /home/cloudera/Desktop/ML_Driver.jar movielens.ML_Join_Driver /user/cloudera/input/ml-latest-small/movies  /user/cloudera/input/ml-latest-small/ratings /user/cloudera/output/movielens/genre_wise_ratings

Output:

Action	3.0908625
Adventure	3.2298608
Animation	3.418974
Children	3.1386745
Comedy	3.190355
Crime	3.3016186
Documentary	3.6867437
Drama	3.4474185
Fantasy	3.1838272
Film-Noir	3.669072
Horror	2.991933
IMAX	3.0590277
Musical	3.334026
Mystery	3.383181
Romance	3.3445897
Sci-Fi	3.1670532
Thriller	3.1830714
War	3.5340207
Western	3.4210017
