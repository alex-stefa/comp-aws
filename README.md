
## Empirical Study of Responsiveness of AWS Regions

<img src="/wiki/title.png" width="600px" />

#### Overview

This is a study that aims to help you decide in which AWS region to place your server for optimum responsiveness.

Assumptions:

 - You run a company website that addresses an audience spread globally but have a small budget.
 - Have funds for only limited number of instances of your website & back end database.

Goals:

Decide optimum placement of an EC2 instance depending on:
 - Predicted response times from users everywhere
 - Presumed geographical distribution of users
 - Maximum cost
    
#### Method

We launch an EC2 instance in each of the 7 [AWS regions](http://aws.amazon.com/about-aws/globalinfrastructure/) and perform HTTP requests from a slice of around 100 [PlanetLab](http://www.planet-lab.org/) nodes distributed globally every 15min for a duration of two weeks. We then collect the data at a central point and analyze the request times to determine which AWS regions are more responsive for clients from various geographical locations.


#### Architecture of the data collection system used in our study

<img src="/wiki/arch.png" width="600px"/>

#### Downloads / Results

 - [Slides explaining the study](/wiki/slides.md)
 - [Plots included in the slides](/wiki/plots.md)
 - [Data set with over 1.9 million data points](https://github.com/alex-stefa/comp-aws/releases/tag/alpha)

---
exported from https://code.google.com/archive/p/comp-aws/
