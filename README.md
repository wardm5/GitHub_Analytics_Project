# GitHub Analytics Project
## Project Idea/Business Value
### Problem Statement
With the expansion of the technology industry over the past few decades, the importance of hiring a qualified developer has never been more important. Due to the demand of seeking highly skilled developers, the demand for recruiters has also risen with many being hired with limited technology experience. The _GitHub Analytics Project_ is designed to help recruiters by providing them a tool to easily see if the candidate matches the experience the company is looking for by analyzing the candidate's GitHub account.

This project will utilize a public dataset containing the repository and commit data of GitHub users. Processing the data with industry standard tools such as Spark, AWS Cloud Computing Platform tools, and PostegSQL, will help show the recruiters the candidates’ skills with summary statistics.

### Business Use Cases
For this project explicitly, the main business use case would be for employers, specifically recruiters. Recruiters could use this website to quickly use the candidate’s GitHub link on their resume and then be able to toggle company standards for that role in order to see how the candidate ranks amongst others on GitHub.

In addition to recruiters using this tool, managers could use this tool for incoming employees in order to understand their strengths and weaknesses and know where they might need more guidance.

Finally, college admissions might find use for this tool as it would help with determining if candidates have enough skills in order to be able to complete coursework in highly technical fields.

### Relevancy
Although this project is especially relevant to the software engineering field, the concepts could be expanded to other fields by using other datasets or APIs. For example, by linking to a LinkedIn API, profile information could be pulled and matched to the candidate’s experience and show similar statistics. Other industries that might find this product useful (although with modifications) are listed below:

* Sports Industry
* Education

## Tech Stack
Although the project is just in its infancy, the projected technology stack components includes the following:

* GitHub dataset (https://bigquery.cloud.google.com/dataset/bigquery-public-data:github_repos?pli=1)
* Apache Spark (data processing)
* Amazon S3 (storage)
* PostegSQL (SQL database for queries)
* EC2 Instance (hosting web application)
* Plotify (frontend)

Potential components are also being considered and listed below:
* LinkedIn API/dataset
* Stack Overflow dataset
* WakaTime API/dataset

The data pipeline for this project is outlined below:
1. Place the *GitHub* dataset into an *Amazon S3* bucket
2. Use *Apache Spark* to process the data into a *PosgreSQL* database
3. Initialize *EC2* instance and use it to host a *web application*
4. Use SQL queries to pull from the database pertinent information and display using *Plotify*

## Data Source
The data source for this project is mainly focusing on the GitHub dataset. The data source was found on Google's BigQuery public datasets and has no restrictions of use. It is important to analyze the data prior to developing and understanding the assumptions being made based on the data.

### Assumptions with data
Bigger commit/project size equals more complexity

### Source Data Statistics
Size: ~770Gb

## Engineering Challenge


## MVP


## Stretch Goals
