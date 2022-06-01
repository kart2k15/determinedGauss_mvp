def create_partition_key(df):
    patient_list = df.select("patient_id").distinct().collect()
    patient_list = [val[0] for val in patient_list] #let's so you've 12000 unique patients
    
    patient_chunks = np.array_split(patient_list, 12) #splits it into 12 chunks of 1000 patients each
    patient_chunks = {i:set(val) for i,val in enumerate(patient_chunks)} # assign seq id to chunks
    
    def get_partitions(chunks_dict, val):
        for chunk_id, chunk in chunks_dict.items():
            if val in chunk:
                return chunk_id
    create_partition_dim = F.udf(lambda x: get_partitions(patient_chunks, x), T.IntegerType())
    
    df = df.withColumn("partition_key", create_partition_dim("patient_id"))
    return df
  
  patient_df = create_partition_key(patient_df)
  patient_df.repartition(300).write.mode("overwrite").partitionBy('partition_key').parquet('s3://lifescience-mvp/Shuks/result_files/')
