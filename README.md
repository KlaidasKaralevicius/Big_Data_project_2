# Big Data Project Nr.2
Create a program using the Spark RDD API approach to extract data from a semi-structured .txt file

## Running code
The corresponding .py file canbe  launched by 'python spark1.py' (or spark2.py/spark3.py) in Anaconda environment. This command should be launched in the same directory where the files are and system should have pyspark installed

## File explanation
- **LD1_data.zip** holds data .txt files used for the project
- **spark1.py**
- **spark2.py**
- **spark3.py**
- **output.txt** result from spark1.py, becous the output is to big to visualise in the terminal

## Timing
In this project, only the  timing of spark3.py was noted, to compare the speed with the Map-Reduce approach on the same data and task
### Timing hardware
- Linux Mint (anaconda environment)
- AMD Ryzen 5 PRO 3500U (4 cores, 8 threads, base speed 2,1 GHz)
- 16 GB RAM (2666 MHz)
- Integrated graphics
Map-Reduce approach - 8 seconds
Spark approach - 34.62 seconds

### P.S.
It should be noted that Map-Reduce notes time only for data mapping, shuffling and reducing, the output is raw .txt file with mapped results, while Spark structures the data into the table besides all the calculations
### P.S.2
Spark timing doesn't involve Spark session initialisation, time counting starts from reading data from file using sparkContext.textFile
