# Smarter BMS development

This repository contains all the package necessary to interact with the DynamoDB database responsible for storing weather, carbon, store BMS and GSHP data for King's Lynn. 


# Prerequisites

1). You must be an authorised user on AWS for this project in order to interact with the database and MQTT broker. Contact mhb316@ic.ac.uk to recieve your AWS login details.\
2). Download and install the [AWS CLI version 2](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html) for your operating system.\
3). Follow these [configuration instructions](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html), using the details given to you in step 1 to allow you to interact with AWS programmatically. \
4). Download and install [Anaconda](https://www.anaconda.com/products/individual) for your operating system.

# Installation
A detailed installation and user guide for first time users can be found in the `first_time_user_guide.ipynb`. For users wishing to install or update their local version, activate your working environment and download using pip as shown below:

`pip install git+https://github.com/MaxBird300/smarter_bms_db.git`

