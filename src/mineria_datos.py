# src/mineria_datos.py
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.tree import DecisionTreeRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, r2_score

def entrenar_modelos(df, objetivo, test_size=0.2, random_state=42):
    X = df.drop(columns=[objetivo])
    y = df[objetivo]

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X.select_dtypes(include='number'))
    X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=test_size, random_state=random_state)

    resultados = {}

    #Regresión Lineal
    rl = LinearRegression()
    rl.fit(X_train, y_train)
    y_pred_rl = rl.predict(X_test)
    resultados['LinearRegression'] = {
        "model": rl,
        "mae": mean_absolute_error(y_test, y_pred_rl),
        "r2": r2_score(y_test, y_pred_rl)
    }

    #Decision Tree
    dt = DecisionTreeRegressor(random_state=random_state)
    dt.fit(X_train, y_train)
    y_pred_dt = dt.predict(X_test)
    resultados['DecisionTree'] = {
        "model": dt,
        "mae": mean_absolute_error(y_test, y_pred_dt),
        "r2": r2_score(y_test, y_pred_dt)
    }

    #Random Forest
    rf = RandomForestRegressor(n_estimators=100, random_state=random_state)
    rf.fit(X_train, y_train)
    y_pred_rf = rf.predict(X_test)
    resultados['RandomForest'] = {
        "model": rf,
        "mae": mean_absolute_error(y_test, y_pred_rf),
        "r2": r2_score(y_test, y_pred_rf)
    }

    return resultados, scaler, (X_train, X_test, y_train, y_test)
