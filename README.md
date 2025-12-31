# data service

![data](data.jpg)

## what is this? 

The data service is the secure go-between between the outside world sending event data to PF and wintermute. It serves the following functions:

* determines whether or not an event message is properly formed
* caches events for datasources not yet initialized
* ensures messages to wintermute are sent in ascending temporal order. 
* tracks datasets, datasources, and metadata on same 
* provides controller/model for front end to manage datasets, datasources, and metadata on same

Service is a Spring Boot microservice. Data store is postgres. 

## how to a build it? 

`mvn clean install` 

Will build the service into a jar as well as create a docker image. 

## how to I make it go? 

The first thing you need to do is build the project. 

Then you'll need to spin up kafka and postgres. Do so by way of the docker-compose file thusly 
.md
`docker-compose up` 

Then you can spin up the data-service from a new terminal thusly

`mvn spring-boot:run` 

## what are the spring-data-rest / queryDSL endpoints and how do I use them? 

tl'dr - some magic is in this service that automagically exposes the database to GET requests along with the means of using queryDSL to make complex queries. 

## where can I get more information? 

This is a PatchFox [turbo](https://github.com/patchfox-io/turbo) service. Click the link for more information on what that means and what turbo-charged services provide to both developers and consumers. 


