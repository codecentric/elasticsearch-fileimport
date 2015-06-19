#elasticsearch-fileimport

Import JSON files directly as documents into Elasticsearch by using the ES transport protocol (not HTTP/REST).
The import is done via bulk requests.

##Prerequisites
* Java 8
* Tested with Elasticsearch 1.6 (should also work with earlier versions)

##Usage

    mvn package
    java -jar target/elasticsearch-fileimport-0.5-SNAPSHOT-jar-with-dependencies.jar file_import_settings.yml

##Configuration
The configuration is done in YAML (or JSON). Look at the following example to see what's configurable:

    fileimport:
        #either transport or node (default: transport)
        mode: node
        #Folder with data
        #One document per file, must end with .json
        #Folder is scanned recursively
        root: import/data
        index: test
        type: test
        #flush_interval: 5s
        max_bulk_actions: 1000
        max_concurrent_bulk_requests: 1
        max_volume_per_bulk_request: 10mb
        #needed when mode: transport
        transport:
            addresses:
                - localhost:9300
                - es.comany.com:9300
                - anothernode:9300
                
    # needed when mode: node
    cluster:
        name: elasticsearch
        
    
    #list other elasticsearch settings here if needed
    #discovery.zen.ping.multicast.enabled: false
    #discovery.zen.ping.unicast.hosts: host:9300
    
##Before you run it
* Install and start your Elasticsearch cluster
* Create your index (with your desired settings)
* Create your type mapping

