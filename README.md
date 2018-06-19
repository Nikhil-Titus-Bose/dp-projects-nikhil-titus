# dp-projects-nikhil-titus

## Long term goals

* Get familiarized with Galapagos and the cloud infrastructure at Bose.  
* Milestone 1: Set up spark on Galapagos and do some benchmarks (benchmarks tests to be identified)
* Milestone 2: Migrate summarization engine written in map reduce to scala and spark. 
* Update on June 19, 2018: Milestones de-prioritized due to more immediate tasks in the Sprint. 


## Tasks completed

* Nifi flow to pull data from Kafka and validate the messages using the validation SDK. Nifi flow was created without installing any dependencies on the cluster. 
* CASTLE-15740: Deployed an elastic search and Kibana cluster using the service brokers available in Galapagos. Also created the nifi flow and introduced a naming convetion for indices which fixed the issue of messages getting rejected in previous nifi flows. 
* CASTLE-13334: Validated UDC messages with the SDK created and triggered and tested some events manually. 

## Work log

### 6/11 - 6/15 

* Worked on configuring eddie with Deepak for validating UDC messages. Awaiting response from Ashish for issues in calling the data api. 
* Worked with Ben on setting up elastic search and kibana. CASTLE-15740