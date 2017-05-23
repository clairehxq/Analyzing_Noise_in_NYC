1. Motivation  
New York City received 408,261 noise complaints in 2016  
Multiple city agencies handle the response to such complaints:
  * NYC 311 collects complaints and stores them in databases
  * NYPD and DEP investigate noise complaint circumstances and issue violations
Complaint response requires substantial resources from municipal government so understanding complaint volume patterns can help inform response strategies

2. Complaints Across NYC  
Normalized Complaint Counts per Census Tract             
![alt text](/viz/normalized_comp_ct.png)  
3. Research Objectives  
Identify Noise Complaint Patterns / Trends at the Census Tract-level
  * Analyze unexplored factors that contribute to noise complaint volumes:
      * hour of day
      * land use zoning features 
      * population demographics
  * Contribute to SONYC project with normalized map of complaint volumes across NYC
4. Data Sources & Pre-Processing  
  - NYC 311 Noise Complaint  | 2010-present  
          Filtered ‘Complaint Type’ to ‘Noise’ & counted volume/ CT by hour, day, month, year, & total
  - mapPLUTO NYC |  2013-2016  
          Filtered to year built, number of floors, assessed total lot value, commercial area, number of buildings per lot,     building depth, and building front  
  - ACS Population Demographics | 2015  
          Median household income, % female, % elderly (65+), % white, % population black, % two races, % hispanic/Latino, and % other race
    - New School Projects | 2013-present  
          School Project start time, end time, building identifier, budget      
5. Modelling
Identify Noise Complaint Patterns / Trends at the Census Tract-level
    
    * Spatial Analysis  
      1. Tested three models that correct for spatial autocorrelation to predict complaint counts 
      2. Selected the optimal model (out of three): Spatial Lag Model 
      3. Selected the optimal weight matrix (out of four): KNN-5 
      
    * Temporal Analysis
      1. Clustered 2,101 Census Tracts by the proportion of noise complaints per hour
      2. Visualized results with color-coded map
      3. Presented CT land use and demographic differences between clusters in bar plots
6. Results
**Spatial Analysis**   
    - Predicted Variable: Complaint Counts per Person
    Spatial Regression Models Tested:  
      Spatial weights: KNN-3, KNN-5, rook, queen  
      Models: Spatial OLS, Spatial Error, Spatial Lag  
    ![spatial correlation tabel](/viz/coef.png)  
    **Optimal Model**  
    Spatial Lag Model with KNN-5 | R-squared: 0.569617418866    
    **Spatial autocorrelation**  
    Moran's I: 0.246903850091 | P-value: 2.71786771858e-50  
    **Conclusion**  
    - Spatial autocorrelation exists, but it is not that strong
    ![alt text](/viz/hotcold.png)  
    - Spatial-autocorrelation identified Census Tracts where the count of complaints were similar to those of adjacent tracts 
    Significance Level: 5%  
    Number of Hotspots: 236 | Coldspots: 424  
**Temporal Analysis**   
  ![alt text](/viz/cluster.png)  
  - Cluster 1 (orange) is comprised of office and commercials buildings in Midtown Manhattan and Downtown Brooklyn. It also includes the CTs surrounding LGA.  
  - Cluster 0 and 2 have no discernible differences (observed in barplot in next slide).  
  ![alt text](/viz/temp_clus.png)
**Matching with School Project**  
  ![alt text](/viz/1_CompCount_Budget_map.png)  
    Complaints align with recorded New School Projects in time and geography contribute to 2% of complaints over the city.  
    Complaint counts correlates with New School Project budget levels.  
7. Big Data Challenges  
**Computing**
  - Spatial merge
    1. Census Tract and mapPLUTO  
    2. Complaints to Census Tract  
    3. Complaints to New School construction projects  
**Storage**
  Major dataset: 5GB  
**Solutions**  
    1. Implementation of R-Tree algorithm  
    2. Efficient spatial merging  
    3. Cloud Computing   
    4. HDFS
    5. Spark
8. Conclusions   
    The features that were most significant in our model include: 
    **Physical**: Building Age, Building Height, AssessTot (value), Commercial Area  
    Areas (CTs) comprised of commercial and office buildings exhibit different hourly complaint trends than do areas with more residential neighborhoods  
    **Demographic**: White, Black, Asian, Hispanic/Latino, Female, Elderly Pop, Median Household Income  
    Population makeup is a good indication of noise complaints volume because every demographic feature we tested was statistically significant.  
    Future models should include a spatial weight as spatial autocorrelation affects noise complaint patterns  

     


