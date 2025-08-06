# 📈 Stock Market Analysis and LSTM-Based Price Prediction

This project involves building a complete ETL pipeline and using an LSTM deep learning model to analyze and forecast stock prices based on historical data. The primary objective is to predict the closing price of a stock using past trends.

---

## 📌 Table of Contents

- [Overview](#overview)
- [Tech Stack](#tech-stack)
- [Project Workflow](#project-workflow)
- [ETL Pipeline](#etl-pipeline)
- [LSTM Model](#lstm-model)
- [Visualizations](#visualizations)
- [Results](#results)
- [Challenges Faced](#challenges-faced)
- [How to Run](#how-to-run)
- [Conclusion](#conclusion)

---

## 📊 Overview

This project demonstrates a real-time stock price prediction system using historical stock data. It includes:
- ETL pipeline to extract, clean, and transform data
- Exploratory Data Analysis (EDA)
- LSTM (Long Short-Term Memory) neural network for time series forecasting
- Visualization and performance evaluation

---

## ⚙️ Tech Stack

- **Language:** Python  
- **Libraries:** Pandas, NumPy, Matplotlib, Seaborn, Scikit-learn, TensorFlow / Keras  
- **Tools:** Jupyter Notebook / VS Code  
- **Data Source:** Yahoo Finance / Kaggle (CSV format)

---

## 🔁 Project Workflow

1. **Extract:** Collect stock data in CSV format  
2. **Transform:** Clean and preprocess the data  
3. **Load:** Save the transformed data for modeling  
4. **Modeling:** Use LSTM to predict future prices  
5. **Evaluation:** Plot and compare actual vs predicted values

---

## 🧼 ETL Pipeline

- **Extract:**  
  Loaded historical stock data using CSV files.

- **Transform:**  
  - Handled missing values  
  - Converted date formats  
  - Engineered new features like:
    - 7-day & 30-day Moving Averages  
    - Daily Returns  
  - Scaled features using `MinMaxScaler` to normalize values in [0, 1] range.

- **Load:**  
  The transformed data was saved as new CSV files or used directly in modeling.

---

## 🧠 LSTM Model

- **Model Type:** Long Short-Term Memory (RNN)  
- **Features Used:** Close price  
- **Window Size:** 60 days (used to predict the next day)  
- **Architecture:**
  - 2 LSTM layers
  - Dropout layer (to prevent overfitting)
  - Dense output layer
- **Loss Function:** Mean Squared Error (MSE)  
- **Optimizer:** Adam  
- **Evaluation Metrics:** RMSE, MAE, Line Chart Visualization

---

## 📈 Visualizations

- Stock closing price over time  
- Moving averages and daily returns  
- Predicted vs Actual closing price (line plot)

---

## 📊 Results

- The LSTM model was able to capture the trends in the data.
- It performed reasonably well in predicting future prices.
- Visualizations showed close alignment between actual and predicted prices.

---

## 🚧 Challenges Faced

- **Overfitting:** Solved using Dropout and early stopping  
- **Scaling:** Resolved using MinMaxScaler  
- **Time-Series Sequencing:** Handled using sliding window technique (last 60 days to predict next)

---

## ▶️ How to Run

1. Clone the repo:
   ```bash
   git clone https://github.com/your-username/stock-price-lstm.git
   cd stock-price-lstm
