# README

## Understanding Cancer Communication Using Big Data Tools

---

## **Introduction**
In today's digital age, where information is abundant, understanding how individuals navigate and trust health-related information is critical. This project focuses on analyzing trust, frustrations, and sentiment regarding healthcare and cancer communication. By utilizing **big data tools and platforms** such as **Azure ML**, **AWS**, and **Apache Spark**, we process and analyze extensive datasets to uncover key insights.

We combine two key datasets:
1. **Health Information National Trends Survey (HINTS):** Structured survey data providing insights into public trust and perceptions of healthcare systems and cancer information.
2. **Reddit Pushshift Dataset:** Free-text data offering an unfiltered look at public discourse on cancer-related topics across various subreddits.

Through **Natural Language Processing (NLP)** and **Machine Learning (ML)**, we aim to:
- Explore public sentiment toward healthcare.
- Investigate trust in healthcare systems.
- Identify emotions like frustration, trust, and joy in cancer-related discussions.
- Build predictive models to classify and analyze cancer-related discussions.

---

## **Big Data Tools and Platforms**

This project leverages **scalable big data tools and platforms** to handle large datasets effectively:
1. **Azure ML:**
   - Hosted data preprocessing and cleaning workflows for Reddit comments.
   - Spark jobs were executed to filter and structure cancer-related subreddit data.
   - Data was stored and processed in Azure Blob Storage.

2. **AWS:**
   - AWS S3 was used to store and access intermediate and final datasets.
   - Pre-trained models for sentiment analysis were hosted and accessed using AWS.

3. **Apache Spark:**
   - **PySpark** was utilized for distributed data processing and transformation.
   - Sentiment analysis and text vectorization (TF-IDF) pipelines were implemented for scalability.
   - Spark's MLlib was employed for model training and evaluation.

---

## **Project Objectives**

1. **Trust in Healthcare Systems:**
   - Analyze whether trust in healthcare systems is influenced by other factors of trust, such as trust in scientists, religious organizations, and government institutions.
   - Predict trust levels using machine learning models like Random Forest and XGBoost.

2. **Sentiment Analysis:**
   - Investigate whether sentiment differs between cancer-related and non-cancer-related subreddits.
   - Perform sentiment classification using **Spark NLP** and measure distributions of positive, negative, and neutral sentiments.

3. **Keyword Analysis:**
   - Identify frequently used words and phrases in cancer-related discussions.
   - Utilize **TF-IDF** to highlight prominent keywords in cancer-focused communities.

4. **Predictive Modeling:**
   - Build machine learning models to classify Reddit comments as cancer-related or non-cancer-related.
   - Compare performance of Naive Bayes and Logistic Regression models for text classification.

---

## **Key Findings**

### **Trust in Healthcare Systems**
- The XGBoost (Tuned) model achieved the highest accuracy (74%) in predicting trust in healthcare systems.
- Trust in scientists showed a strong correlation with overall trust in the healthcare system.

### **Sentiment in Subreddits**
- Cancer-related subreddits had a higher proportion of positive sentiment compared to non-cancer subreddits.
- Emotional analysis revealed heightened levels of trust and sadness in cancer-related subreddits.

### **Keyword Analysis**
- Common words like "time," "doctor," and "information" were prominent in cancer-related discussions.
- Keywords suggest a focus on treatment logistics, support, and gratitude.

### **Predictive Modeling**
- Naive Bayes outperformed Logistic Regression in classifying Reddit comments, achieving an accuracy of 78%.
- Logistic Regression showed limitations in capturing nuances of cancer-related discussions.

---

## **Conclusion**

This project highlights the value of integrating big data tools and advanced analytics to enhance understanding of public sentiment and trust in healthcare systems. By combining structured survey data (HINTS) with unstructured Reddit data, we provide a holistic view of cancer communication. The findings can inform healthcare providers, policymakers, and researchers in improving communication strategies and fostering trust in healthcare.

---

## **Acknowledgments**
This project was conducted as part of the Fall 2024 course at Georgetown University. Special thanks to our instructors and collaborators for their guidance and support.

---

## **License**
This project is licensed under an **All Rights Reserved** license. Unauthorized use, distribution, or modification of the code is strictly prohibited. For inquiries or permission requests, please contact [msheeba00@gmail.com or sm3924@georgetown.edu].

