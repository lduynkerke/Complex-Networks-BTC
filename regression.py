from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression, Lasso, Ridge
from sklearn.metrics import mean_squared_error, r2_score
import numpy as np


def prepare_data(dataframe):
    # Shift rows by one in order to predict values
    x = dataframe[['size', 'witness', 'transaction_count', 'difficulty', 'chainwork']].iloc[:-1].values
    y = dataframe[['price', 'volatility', 'volume']].iloc[1:].values
    print(x.shape, y.shape)

    # Shuffle the concatenated data
    indices = np.random.permutation(len(x))
    x_shuffled = x[indices]
    y_shuffled = y[indices]

    # Split the data into training and testing sets
    x_train, x_test, y_train, y_test = train_test_split(x_shuffled, y_shuffled, test_size=0.2, random_state=42)

    # Column names
    y_cols = ['price', 'volatility', 'volume']

    return x_train, x_test, y_train, y_test, y_cols


def fit_linear_model(x_train, x_test, y_train, y_test):
    linear_model = LinearRegression()
    linear_model.fit(x_train, y_train)

    # Evaluate the model
    predict_linear = linear_model.predict(x_test)
    mse_linear = mean_squared_error(y_test, predict_linear)
    r2_linear = r2_score(y_test, predict_linear)

    # Print the mean squared error, intercept and coefficients
    print("\nLinear Regression Model")
    print("Mean Squared Error:", mse_linear)
    print("Intercept: ", linear_model.intercept_)
    print("Coefficients: ", linear_model.coef_)
    print("R^2:", r2_linear)

    return linear_model


def fit_lasso_model(x_train, x_test, y_train, y_test):
    lasso_model = Lasso(alpha=0.1)  # Alpha is the regularization strength, adjust as needed
    lasso_model.fit(x_train, y_train)

    # Evaluate the model
    predict_lasso = lasso_model.predict(x_test)
    mse_lasso = mean_squared_error(y_test, predict_lasso)
    r2_lasso = r2_score(y_test, predict_lasso)

    # Print the mean squared error, intercept and coefficients
    print("\nLasso Regression Model")
    print("Mean Squared Error:", mse_lasso)
    print("Intercept:", lasso_model.intercept_)
    print("Coefficients:", lasso_model.coef_)
    print("R^2:", r2_lasso)

    return lasso_model


def fit_ridge_model(x_train, x_test, y_train, y_test):
    ridge_model = Ridge(alpha=1.0)  # Alpha is the regularization strength, adjust as needed
    ridge_model.fit(x_train, y_train)

    # Evaluate the model
    predict_ridge = ridge_model.predict(x_test)
    mse_ridge = mean_squared_error(y_test, predict_ridge)
    r2_ridge = r2_score(y_test, predict_ridge)

    # Print the mean squared error, intercept and coefficients
    print("\nRidge Regression Model")
    print("Mean Squared Error:", mse_ridge)
    print("Intercept:", ridge_model.intercept_)
    print("Coefficients:", ridge_model.coef_)
    print("R^2:", r2_ridge)

    return ridge_model


def fit_regression(x_train, x_test, y_train, y_test, y_cols):
    for column in range(len(y_cols)):
        print('\nTest models for: ', y_cols[column])
        fit_linear_model(x_train, x_test, y_train[:, column], y_test[:, column])
        fit_lasso_model(x_train, x_test, y_train[:, column], y_test[:, column])
        fit_ridge_model(x_train, x_test, y_train[:, column], y_test[:, column])


def bootstrap(x_train, x_test, y_train, y_test, model):
    mse_scores = []
    coefficients_list = []
    intercept_list = []

    for i in range(1000):
        # Generate random indices with replacement for the bootstrap sample
        bootstrap_indices = np.random.choice(len(x_train), len(x_train), replace=True)

        # Create bootstrap sample
        x_bootstrap = x_train[bootstrap_indices]
        y_bootstrap = y_train[bootstrap_indices]

        # Fit Lasso Regression model to the bootstrap sample and predict
        model.fit(x_bootstrap, y_bootstrap)
        predictions = model.predict(x_test)

        # Calculate mean squared error and append to list
        mse = mean_squared_error(y_test, predictions)
        mse_scores.append(mse)

        # Store coefficients and intercept
        coefficients_list.append(model.coef_)
        intercept_list.append(model.intercept_)

    # Calculate the mean and standard deviation of mean squared errors
    mean_mse = np.mean(mse_scores)
    std_mse = np.std(mse_scores)

    # Print the mean and standard deviation of mean squared errors
    print("\nBootstrapped")
    print("Mean MSE:", mean_mse)
    print("Std MSE:", std_mse)

    # Convert lists to numpy arrays for easier manipulation
    coefficients_array = np.array(coefficients_list)
    intercept_array = np.array(intercept_list)

    # Print mean and standard deviation of coefficients and intercepts
    print("Mean Coefficients:", np.mean(coefficients_array, axis=0))
    print("Std Coefficients:", np.std(coefficients_array, axis=0))
    print("Mean Intercept:", np.mean(intercept_array))
    print("Std Intercept:", np.std(intercept_array))
