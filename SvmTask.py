
# coding: utf-8

# In[1]:

# FIRST SOME MESOS ADMIN

import json
import urllib2
import dns.resolver

MESOS_LEADER_URL = "leader.mesos"

def get_cassandra_nodes():
    cassandra_service_url = MESOS_LEADER_URL
    try:
        answers = dns.resolver.query('_cassandra._tcp.marathon.slave.mesos', 'SRV')
        service_url = answers[0]
        
        domain = str(service_url.target)[:-1] # Remove trailing .
        port = str(service_url.port)
        cassandra_service_url = domain + ":" + port
        print cassandra_service_url
    except Exception as e:
        print e
        service_name = "cassandra"
        cassandra_service_url += "/service/" + service_name

    url = "http://" + cassandra_service_url + "/v1/nodes/connect"
    zookeeper_response = urllib2.urlopen(url).read()
    json_object = json.loads(zookeeper_response)
    return [host.split(':')[0] for host in json_object["address"]]


# In[2]:

## General Cassandra queries
# http://datastax.github.io/python-driver/getting_started.html

from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy

cassandra_nodes = get_cassandra_nodes()
cluster = Cluster(cassandra_nodes, load_balancing_policy=DCAwareRoundRobinPolicy(), connect_timeout=20, port=9042, max_schema_agreement_wait=20, control_connection_timeout=5.0)
session = cluster.connect()
    
def quick_cql(query, params={}):
    results = session.execute(query, params)
    for row in results:
        print row
    print 'executed CQL'
        


# In[3]:

# Let's connect to Cassandra
# THIS DOES NOT USE SPARK AT ALL

# This method should not be used for reading/writing data
# Just use for schema changes and debugging

quick_cql(
    """
    SELECT keyspace_name, columnfamily_name FROM system.schema_columnfamilies
    """
)


# In[4]:

# MORE SIMPLE CASSANDRA QUERIES
my_keyspace = 'dev_mena1'

quick_cql(
    """
    CREATE KEYSPACE IF NOT EXISTS "%s"
          WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'dc1' : 2 }
          AND DURABLE_WRITES = true
    """ % (my_keyspace,)
)


# In[5]:

print 'welcome to Tractable PySpark'


# In[6]:

# More spark / mesos admin
import os
import logging

KEY = 'PYSPARK_SUBMIT_ARGS'
os.environ[KEY] = " --proxy-user core --packages TargetHolding:pyspark-cassandra:0.3.5,datastax:spark-cassandra-connector:1.6.0-M1-s_2.10,com.databricks:spark-csv_2.10:1.3.0,com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws:2.6.0 pyspark-shell "

KEY = 'PYSPARK_DRIVER_PYTHON'
os.environ[KEY] = '/usr/local/bin/ipython'

KEY = 'PYSPARK_PYTHON'
os.environ[KEY] = '/usr/bin/python'

SPARK_HOME = 'SPARK_HOME'
os.environ[SPARK_HOME] = '/opt/spark/dist'

SPARK_USER = 'SPARK_USER'
os.environ[SPARK_USER] = 'core'

KEY = 'MESOS_NATIVE_JAVA_LIBRARY'
os.environ[KEY] = '/usr/local/lib/libmesos.so'

print os.environ[KEY]

import sys
sys.path += [os.environ[SPARK_HOME] + "/python/"]
sys.path += [os.environ[SPARK_HOME] + "/python/lib/py4j-0.9-src.zip"]


# In[7]:

# Check pyspark is on the pythonpath
import pyspark


# In[8]:

# Function to create a spark context - only execute this once
from pyspark import SparkContext, SparkConf, SQLContext

def configure_spark_context(application_name, spark_cores=None, spark_log_level="WARN"):

    assert application_name is not None
    assert len(application_name) > 0

    conf = SparkConf()
    conf.setMaster("mesos://zk://{url}:2181/mesos".format(url=MESOS_LEADER_URL))

    conf.setAppName(application_name)
    conf.set("spark.executor.memory", "20g")
    conf.set("spark.executor.uri", "http://downloads.mesosphere.io.s3.amazonaws.com/spark/assets/spark-1.6.0.tgz")
    
    # MESOS SETTINGS
    #conf.set("spark.mesos.extra.cores",2)
    conf.set("spark.mesos.executor.memoryOverhead", "4096")
    conf.set("spark.mesos.executor.docker.image","tractableio/mesos-spark-executor:0.0.6")

    
    # CASSANDRA PROPERTIES
    # see http://docs.datastax.com/en/latest-dse/datastax_enterprise/spark/sparkCassProps.html
    conf.set("spark.cassandra.connection.host", ','.join(get_cassandra_nodes()))
    conf.set("spark.cassandra.connection.keep_alive_ms","30000")
    conf.set("spark.cassandra.input.fetch.size_in_rows","1000")
    conf.set("spark.cassandra.input.split.size_in_mb", "64")
    conf.set("spark.cassandra.input.metrics", "false")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    #conf.set("spark.cassandra.connection.compression","NONE")

    # This is the TOTAL number of cores in the entire cluster to allocate (NOT the number of cores per executor)
    # It seems that spark tries to spread itself around as much as possible.
    # if max < mesos slaves then it takes 1 core each on max slaves.
    # first tries to get 1 core on each instance and then starts taking more cores
    spark_cores = spark_cores if spark_cores else len(cassandra_nodes) * 2

    assert int(spark_cores) > 0
    conf.set("spark.cores.max",str(spark_cores))

    #conf.set("spark.executor.cores","9")
    # TODO: work out if this is possible?
    # conf.set("spark.mesos.constraints", "cassandra.dcos:true")
    # conf.set("spark.mesos.constraints",'cpu:2') # This doesnt seem to work

    sc = SparkContext(conf=conf)
    sc.setLogLevel(spark_log_level)

    return sc


# In[9]:

def read_from_cassandra(sqlContext, keyspace_name, table_name):
    dataframe = sqlContext.read.format("org.apache.spark.sql.cassandra")         .options(table=table_name, keyspace=keyspace_name)         .load()
    return dataframe


# In[10]:

def write_dataframe_to_cassandra(sqlContext, keyspace_name, table_name, dataframe):
    dataframe.write.format("org.apache.spark.sql.cassandra")         .mode('append')         .options(table=table_name, keyspace=keyspace_name)         .save()
        
def write_rdd_to_cassandra(sqlContext, keyspace_name, table_name, column_schema, rdd):
    dataframe = sqlContext.createDataFrame(rdd,schema=column_schema)
    write_dataframe_to_cassandra(sqlContext, keyspace_name, table_name, dataframe)


# In[11]:

# Start a SparkContext
sc = configure_spark_context('mena\'s notebook', 3)
sqlContext = SQLContext(sc)


# In[12]:

# Initialize SparkContext + check that it is working by generating some random data
#http://spark.apache.org/docs/latest/api/python/pyspark.html

# warm Spark up
x_rdd = sc.parallelize(range(1000))
two_x_rdd = x_rdd.map(lambda x: x*2)
result = two_x_rdd.collect()
print 'done', result[999] == 1998


# In[30]:

# Set up Cassandra Schema and generate some test data

import random
import uuid
import numpy  
import time
from pyspark.sql.types import *

# Set up schema in Cassandra
features_table_name = 'feature_vectors_0'

quick_cql(
    "CREATE TABLE IF NOT EXISTS \"%s\".\"%s\" ( image_id uuid, feature_set_id uuid, feature_vector frozen<list<float>>, PRIMARY KEY ((image_id),feature_set_id) ) WITH caching = { 'keys' : 'ALL', 'rows_per_partition' : '100000' };" % (my_keyspace,features_table_name)
)

features_schema = StructType([
    StructField('image_id', StringType(), True), 
    StructField('feature_set_id', StringType(), True),
    StructField('feature_vector', ArrayType(StringType()), True)
])


labels_table_name = 'image_labels_0'
quick_cql(
    "CREATE TABLE IF NOT EXISTS \"%s\".\"%s\" ( image_id uuid, experiment_id uuid, labels map<text,float>, PRIMARY KEY ((image_id),experiment_id) ) WITH caching = { 'keys' : 'ALL', 'rows_per_partition' : '100000' };" % (my_keyspace,labels_table_name)
)

labels_schema = StructType([
    StructField('image_id', StringType(), True), 
    StructField('experiment_id', StringType(), True),
    StructField('labels', MapType(StringType(), FloatType()), True)
])


# Now generate fake data
# using small data sizes for testing, 
# larger data sizes require more resources and are slower

# Values:
num_dim=1024
num_feat_sets=2
num_images=1000
experiment_id = str(uuid.uuid4())
labels = ['cat', 'dog', 'hamster']

def generate_random_w():
    a = tuple([random.uniform(0, 1) for _ in range(num_dim+1)])
    return a

def generate_uuids():
    return str(uuid.uuid4())

def generate_random_feature_vectors():
    import random
    a = tuple([random.uniform(0, 1) for _ in range(num_dim)])
    return a

def generate_random_labels():
    import numpy
    
    # Some labels have value 0, others have value 1, others have no value (i.e. key is missing means unlabelled)
    chosen = numpy.random.choice(labels, size=2, replace=False)
    d = dict()
    # only label half the data
    if random.randint(0,9) < 5:
        d[str(chosen[0])] = 1.0
        
    d[str(chosen[1])] = 0.0
    return d

def generate_my_labels(v, w):
    d = dict()
    # only label half the data
    if random.randint(0,9) < 5:
        score = w[num_dim]
        for i in range(num_dim):
            score = score + (v[i] * w[i])
        label = 0
        if score > 0:
             d['cat'] = 1
        else:
            d['dog'] = 1
    d['dog'] = 0.0
    return d

print 'preparing data'
time_0 = time.time()
feature_set_ids = map(lambda x: generate_uuids(), range(num_feat_sets))

input_rdd = sc.parallelize(range(num_images), 24).flatMap(lambda y: [(generate_uuids(), id) for id in feature_set_ids])
cached = input_rdd.cache() 
w = generate_random_w()
try:
    cached.count()
    features_rdd = cached.map(lambda x: x + (generate_random_feature_vectors(),))
    #labels_rdd = cached.map(lambda x: (x[0], experiment_id, generate_random_labels()))
    
    labels_rdd = cached.map(lambda x: (x[0], experiment_id, generate_my_labels(x[0], w)))
    
    print time.time() - time_0
    time_0 = time.time()

    print 'checking for existing data'
    
    df_feats = read_from_cassandra(sqlContext, my_keyspace, features_table_name)
    df_labels = read_from_cassandra(sqlContext, my_keyspace, labels_table_name)
    row_count = df_feats.count() + df_labels.count()
    
    print time.time() - time_0
    print 'existing rows'
    print row_count
    time_0 = time.time()
    
    if row_count == 0:
        write_rdd_to_cassandra(sqlContext, my_keyspace, features_table_name, features_schema, features_rdd)
        write_rdd_to_cassandra(sqlContext, my_keyspace, labels_table_name, labels_schema, labels_rdd)
        print 'wrote data to cassandra'
        print time.time() - time_0
    elif row_count != 2* num_feat_sets * num_images:
        raise Error("existing data is wrong size for this test")
    else:
        print 'assuming existing data is correct, skipping write'

finally:
    cached.unpersist()

print 'features table'
read_from_cassandra(sqlContext, my_keyspace, features_table_name).show()

print 'labels table'
read_from_cassandra(sqlContext, my_keyspace, labels_table_name).show()

print my_keyspace
print features_table_name
print labels_table_name


# In[31]:

table_name = features_table_name
print 'getting a feature set id'
time_0 = time.time()

source_df = read_from_cassandra(sqlContext, my_keyspace, table_name)
feat_set_id = source_df.select(source_df['feature_setlabels_rdd_id']).take(1)[0].feature_set_id
print time.time() - time_0


print 'load features into spark'
time_0 = time.time()

df = source_df.filter(source_df['feature_set_id'] == feat_set_id).select(source_df['image_id'], source_df['feature_vector'])
#df.show()
rdd = df.map(lambda x: (x.image_id, numpy.array(x.feature_vector,dtype=numpy.float64))) 

# We maintain data for the below operations:
cached_rdd = rdd.cache()
try:
    # This count forces Spark to evaluate the RDD, otherwise it is lazy and does nothing
    print cached_rdd.count()
    print time.time() - time_0
    time_0 = time.time()
    
    pass
finally:
    cached_rdd.unpersist()



# In[ ]:




# # Notes
# 
# 
#  - You will need to create the output table in Cassandra using a create table statement.
#  
#  - To better test the SVM, once code is working, you could change the way the test features and labels are generated so that there is a relationship between the labelled class and the features vector.

# In[ ]:


        


# In[ ]:




# In[32]:

from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col

keys = ['cat', 'dog', 'hamster']

def prepareData():
    df_feats = read_from_cassandra(sqlContext, my_keyspace, features_table_name)
    df_labels = read_from_cassandra(sqlContext, my_keyspace, labels_table_name)
   
    feat_set_id = df_feats.select(df_feats['feature_set_id']).take(1)[0].feature_set_id
    print 'Using feature set id : '
    print feat_set_id
   
    # udf_label = udf(getClassLabel, IntegerType())
    # dx = df_labels.withColumn('class_labels', udf_label(df_labels['labels']))
    #dx.show()//
    res_df = df_feats.filter(df_feats['feature_set_id'] == feat_set_id).join(df_labels, df_feats.image_id == df_labels.image_id).select(df_feats['image_id'], df_feats['feature_vector'], df_labels['labels'])
    #res_df_labeled = res_df.withColumn('classLabel', getClassLabel(res_df.labels))
    exprs = [col("labels").getItem(k).alias(k) for k in keys]
    trans_data = res_df.select(res_df.image_id,res_df.feature_vector, *exprs)
    trans_data = trans_data.na.fill(0)
   
    return trans_data
 


# In[39]:

def createResTable(results_table_name):
    quick_cql("CREATE TABLE IF NOT EXISTS \"%s\".\"%s\"( query_id uuid, score float, image_id uuid, PRIMARY KEY ((query_id), score) ) WITH caching = { 'keys' : 'ALL', 'rows_per_partition' : '100000' };"% (my_keyspace, results_table_name)
    )
    results_schema = StructType([
        StructField('query_id', StringType(), True),
        StructField('image_id', StringType(), True),
        StructField('score', FloatType(), True)
    ])
    return results_schema


# In[48]:


from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.mllib.classification import SVMWithSGD, SVMModel
from pyspark.mllib.regression import LabeledPoint
from pyspark.sql.functions import col

#Paramters to tune
my_iterations = [5000];
my_regParam = [0.01, 0.001]
my_step = [ 2**j for j in range(-3, -2) ]
numFolds=3

print 'prepare data'
trans_data = prepareData()
labeled_df = trans_data.where((trans_data.cat == 1) | (trans_data.dog == 1) | (trans_data.hamster == 1)).withColumn('label', F.when( (trans_data.cat == 1.0), 1.0).otherwise(0.0))

#Randomly shuffle data
#labeled_df.orderBy(rand())
test_df = trans_data.where((trans_data.cat != 1) & (trans_data.dog != 1) & (trans_data.hamster != 1))


val_range = [ j for j in range(0, numFolds) ]
split_range =  [ 1.0/numFolds for j in range(0, numFolds)]
gl_valid_err = 2
best_i = 0
best_j = 0
best_k = 0

split_data = labeled_df.randomSplit(split_range)
split_train_data = []
split_val_data = []
for t in val_range:
    split_val_data.insert(t, split_data[t])
    for v in val_range:
        if t!=v:
            if len(split_train_data) > t:
                split_train_data[t] = split_train_data[t].unionAll(split_data[v])
            else:
                split_train_data.insert(t, split_data[v])

print 'data prepared'

for i in my_iterations:
    for j in my_regParam:
        for k in my_step:
            validErr = 0
            for v in val_range:
                svmmodel = SVMWithSGD.train(split_train_data[v].select(col("label"), col("feature_vector")).map(lambda row: LabeledPoint(row.label, row.feature_vector)),
                                    iterations=i, regParam=j,step=k)
                valid_pred = split_val_data[v].map(lambda p: (p.label, svmmodel.predict(p.feature_vector)))
                validErr += valid_pred.filter(lambda (a, p): a != p).count() / float(valid_pred.count())
            validErr = validErr/ numFolds
            print 'Iterations : ' + str(i) + ' RegParam : ' +   str(j) +  ' Step size : ' + str(k) + ' Validation Error : ' + str(validErr)
            if validErr < gl_valid_err:
                gl_valid_err = validErr
                best_i = i
                best_j = j
                best_k = k

svmmodel = SVMWithSGD.train(labeled_df.select(col("label"), col("feature_vector")).map(lambda row: LabeledPoint(row.label, row.feature_vector)),
                            iterations=best_i, regParam=best_j, step=best_k)
'''
svm = SVMWithSGD.train(labeled_df.select(col("label"), col("feature_vector")).map(lambda row: LabeledPoint(row.label, row.feature_vector)),iterations=5000, regParam=0.01,step=0.125)
paramGrid = ParamGridBuilder().build()

evaluator = BinaryClassificationEvaluator()

cv = CrossValidator(estimator=pipeline, estimatorParamMaps=paramGrid, evaluator=evaluator, numFolds=5)
cvmodel = cv.fit(labeled_df)
'''

print('Evaluation ....')

# Evaluating the model on training ddf_feats = read_from_cassandra(sqlContext, my_keyspace, features_table_name)ata
train_pred = labeled_df.map(lambda p: (p.label, svmmodel.predict(p.feature_vector)))
trainErr = train_pred.filter(lambda (a, p): a != p).count() / float(train_pred.count())
print("Training Error = " + str(trainErr))

#valid_pred = validate_df.map(lambda p: (p.label, svmmodel.predict(p.feature_vector)))
#validErr = valid_pred.filter(lambda (a, p): a != p).count() / float(valid_pred.count())
#print("Validation Error = " + str(validErr))

def generate_uuid():
    return str(uuid.uuid4())
svmmodel.clearThreshold()
testrow = test_df.map(lambda p: (generate_uuid(), p.image_id, svmmodel.predict(p.feature_vector).tolist())).collect()
  
results_table_name = 'result_vectors_0'
results_schema = createResTable(results_table_name)
write_rdd_to_cassandra(sqlContext, my_keyspace, results_table_name, results_schema, testrow)


# In[49]:

table_name = results_table_name
print 'OUTPUT'
time_0 = time.time()

source_df = read_from_cassandra(sqlContext, my_keyspace, table_name)
df = source_df.select(source_df['query_id'], source_df['image_id'], source_df['score'])
df.show(20)


# In[ ]:



