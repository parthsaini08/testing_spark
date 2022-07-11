# Codebase for the Spark

## Local development environment setup instructions

Following are the steps to start consuming from the Kafka consumer:

- Install Spark on your local system. Click [here](https://phoenixnap.com/kb/install-spark-on-ubuntu) for a step-by-step guide to install Spark on Ubuntu.<br><br>
- Run the following command to install the pyspark dependancy using PyPI</br><br>
  `pip install pyspark`  </br><br>
- Add the config.ini file to the `/src` directory (check the section below)***<br> Change the config.ini file according to your Kafka bootstrap server, API-key and API-secret.<br>*** {Optional} <br>Change other configuration options based on preference.<br><br>
- Run the `runAll.sh` bash file from command line using the following command to run all the use-cases together. For eg:- </br>
`bash runAll.py`</br></br>
- To stop all processes, run the `stopAll.sh` file from terminal. This will stop all instances of the `spark_processor.py` file.

</br>

### To run a single use-case:
Navigate to the `/src` directory and run the command below. Following are the arguments to pass through the command line.

```
usage: spark_processor.py [-h] [-k KAFKA] [-s SPARK]
                          [-o OPERATION]

optional arguments:
  -h, --help            show this help message and exit
  -k KAFKA, --kafka KAFKA
                        Pass the kafka version as a
                        parameter. Default value : 2.12
  -s SPARK, --spark SPARK
                        Pass the spark version as a
                        parameter. Default value : 3.2.0
  -o OPERATION, --operation OPERATION
                        Pass the operation to be done as
                        a STRING. Currently supported
                        operations :
                        1. customers_count
                        2. transactions_count
                        3. accounts_count
                        4. price_at_an_instant
                        5. product_relative_reach
                        6. net_wealth
                        7. trending_list_of_products
                        8. trending_list_of_symbols
                        9. age_demographic
```
*If no version is provided for Kafka and Spark, default parameters are assumed.* </br>
`Spark version - 3.2.0`  </br>
`Kafka version - 2.12`
</br>

---
### Configurating your Confluent Kafka access
</br>

Create a `config.ini` file in the **`/src`** directory. Add the following parameters after filling up the Confluent Kafka cluster details to the file

```
[default]
bootstrap.servers= <YOUR BOOTSTRAP SERVER>
security.protocol=SASL_SSL
sasl.mechanisms=PLAIN
sasl.username= <YOUR API KEY>
sasl.password= <YOUR API SECRET>
auto.offset.reset=earliest

# 'auto.offset.reset=earliest' to start reading from the beginning of the topic if no committed offsets exist.
# default protocols assumed for auto.offset.reset, sasl.mechanisms and security.protocol. Change according to preference.
```
---
## Development Instruction

The `spark_processor.py` file has been created to separate the feature files and the processing unit, for ease of development. In order to add an additional feature, developers have to :

- Create a python file named in the format `spark_<feature name>.py`.
- Create a class that takes 3 parameters to it's `__new__` function.<br>
    a) The spark instance<br>
    b) The base dataframe<br>
    c) The topic name<br>

    and returns a dataframe after computing the necessary to the spark processor.
- In the `dictionary.py` file,
    - Import the python file created.
    - Add a key-value pair to the `dictionary` python dictionary, in the following format <br>
    `"<feature name>": {"input_topic": "<topic>", "input_reading_offset":<offset>, "function": <fileName.className>, "output_mode":"<output_mode>"}`<br><br>
    >Example :

    ```
        "customers_count": {
            "input_topic": "customers",
            "input_reading_offset": "earliest",
            "function": sp_dc.countDocs,
            "output_mode": "complete",
        }
    ```
    *Note:*<br>
    - *The feature name should be the same as the feature name given to the spark file created*
    - *The topic should be the topic you want the functions to process on. If the topic is not related, schema fields may not be found and spark_processor will throw an error*
    - *The fileName parameter should be as imported. className is the class created in the file*
    - *The input reading offset should be selected according to use case*
    - *output_mode has to be selected according to the operation or preference.*
<br><br>
---

## Class Diagram
### *For the **Price at an instant** use-case*

![Spark class diagram for Price At An Instant](../../assets/spark/spark_class_diagram.svg?raw=true "Spark Class Diagram")
- The `sparkInitializer` class takes the name of the app, the Kafka version and the Spark version
- `configure` class returns a dictionary with Kafka Confluent credentials and connection information after parsing the `config.ini` file
- `sparkConsumer` takes the appName, Kafka and Spark version, and has one instance each of the `sparkInitializer` and `configure` and returns the Spark session and context to the use-case class, which is `PriceAtAnInstant` in this case.
- `KafkaProducer` has one instance of the configure class. It contains the `produce` function that takes the result dataframe, the mode of publishing to the topic and the operation name in order to determine which topic to write to.
- `PriceAtAnInstant` is an example use-case class, having one ( but can have more ) instances of the consumer and producer classes.
- All use-case classes take the Spark Instance returned from the consumer class, the base dataframe and the topic, and return a streaming dataframe.

---
## Directory structure

Spark has the following directory structure :

```
Spark
├── operationList.txt
├── Readme.md
├── runAll.sh
├── src
│   ├── config.ini
│   ├── spark_config.py
│   ├── spark_consumer.py
│   ├── spark_processor.py
│   ├── spark_producer.py
│   └── use_cases
│       ├── spark_age_demographic.py
│       ├── spark_aggregate_transactions.py
│       ├── spark_document_count.py
│       ├── spark_get_high_risk_accounts.py
│       ├── spark_net_wealth.py
│       ├── spark_price_at_an_instant.py
│       ├── spark_product_relative_reach.py
│       ├── spark_trending_list_of_products.py
│       └── spark_trending_symbols.py
├── stopAll.sh
└── tests
    ├── operationDictionary.py
    ├── schemaDictionary.py
    ├── spark_test.py
    └── useCase_test.py

3 directories, 22 files
```
---
