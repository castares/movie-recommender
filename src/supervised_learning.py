# Dask
import dask.dataframe as dd
from dask_ml.cluster import SpectralClustering
from dask_ml.cluster import KMeans
from dask_ml.model_selection import train_test_split, GridSearchCV, IncrementalSearchCV

# Sklearn
from sklearn.pipeline import Pipeline
from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_error
from sklearn import svm, linear_model, tree
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor, AdaBoostRegressor


def defineXy(ratings_df):
    to_X = [e for e in ratings_df.columns if ratings_df[e].max() <=
            1 and ratings_df[e].max() > 0]
    X = ratings_df[to_X]
    y = ratings_df['rating']
    return X, y


def mlmodelSelection(models_dict, X_train, X_test, y_train, y_test):
    for modelName, model in models_dict.items():
        print(f"\nTraining model: {modelName}")
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)
        print("RSME", (mean_squared_error(y_test, y_pred)**0.5))
        print("MAE", mean_absolute_error(y_test, y_pred))
        print("r2_score", r2_score(y_test, y_pred))


def main():
    pass


if __name__ == "__main__":
    main()
