# spark_compaction

**Introduction:**
    
    This Application compacts the files in HDFS
    
**Set the Environment and Usage:**

   1. Clone the "spark_compaction" project from the git repo https://github.com/aravindboppana/spark_compaction.git
    
   2. Navigate to the place where pom.xml file is present and run the following command. This builds a jar file with all the dependencies under target directory.
            
            mvn clean install

   3. Navigate to the directory where you want to deploy this application. Run the following commands to create the required directories to set the environment for the project. 
            
            mkdir bin
            mkdir conf
            mkdir lib 
   
   4. Now do the following steps to copy the files to the project directory.
            
            a. Copy the application_configs.json file from the resources dir in the project to conf directory. Update the application.json file with the required configurations.
            b. Copy the jar file that was created in the step 2 to lib directory.
            c. Copy the shell script under bin dir in the project to bin directory that you have created. Update the script "spark_compaction.sh" with the  placeholders. Change the permissions for the file if necessary.
            
            Now the environment is set to run the  application using the script "spark_streaming_to_kudu.sh". Instructions to run the script is provided under help section in the shell script.