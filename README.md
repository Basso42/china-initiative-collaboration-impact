# China Initiative and Scientific Collaboration Networks

This repository contains the code and data processing scripts for the project *"The China Initiative and the Disruption of Scientific Collaboration."*  
The project analyzes the impact of the U.S. China Initiative on research collaborations between American and Chinese scientists, focusing on the field of Pulmonary and Respiratory Medicine (2018–2020).  
Using bibliometric data from OpenAlex and network analysis techniques, we document significant declines in coauthorship, research productivity, and network centrality among highly exposed U.S. researchers after 2018.  
The repository includes data preparation scripts, network construction, visualization, and statistical analysis tools to reproduce the results presented in the study.

## Reproductibility

To avoid overloading the OpenAlex API, we have pre-uploaded the publication dataset used in this project.

The dataset contains **81,354 articles** from the **Pulmonary and Respiratory Medicine** field, published between **January 1, 2016** and **March 15, 2020**. It includes **52 variables** such as DOI, title, authorship information, grant details, and more. The data is stored in a **Parquet file (~380 MB)**, which can be accessed here:

➡️ [Download the dataset](https://minio.lab.sspcloud.fr/gamer35/public/all_works_16_20.parquet)

To set up the environment and download the dataset automatically, run the following command:

```bash
bash init.sh

## License
This project is released under the MIT License.
