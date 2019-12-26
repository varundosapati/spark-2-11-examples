Apache spark using scala with Machine Learning (MLlib)

What it does ?

It performs k-means clustering on text messages streamed from slack. It may use an existing k-means model or new training data may be used and used to train


How it works ?

There are three cases to run the code depending on the config variables passed to the program 

1. if --trainData and --modelLocation config variables are specified, the program will train model based on existing train data in --trainData 
(example trainData is in the input/output)

2. if --trainData and not --slackToken, the program will just train model and save it if --modelLocation specified 

3. if --slackToken and --modelLocation specified, the program will load model from the --modelLocationand put it to Streaming app which will be used for near real time prediction    



