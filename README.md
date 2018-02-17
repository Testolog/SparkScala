# SparkScala
spark should be 1.6
## requirment opts to run
<path_to_file> <output_schema> <output_path>

## for azure need to compile with <p>
  \<dependency><p>
      <t>\<groupId>org.apache.hadoop\</groupId><p>
      <t>\<artifactId>hadoop-azure\</artifactId><p>
      <t>\<version>$varsion\</version><p>
  \</dependency><p>

## for run on azure cluster need to provide next parameters for spark configuration
--conf fs.azure="org.apache.hadoop.fs.azure.NativeAzureFileSystem" --conf fs.azure.account.key.<youraccount>.blob.core.windows.net=<access_key>

