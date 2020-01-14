import pandas as pd
from pyspark.ml.regression import LinearRegression

def lr_summary(lrmodel, features):
    '''
    Prints out statistics for a trained Spark LinearRegression model. 
    '''
    print("MODEL SUMMARY")
    print()
    print(pd.DataFrame({
        "Features": features + ["constant"],
        "Coefficients": list(lrmodel.coefficients) + [lrmodel.intercept],
        "Standard errors": lrmodel.summary.coefficientStandardErrors,
        "t-Values": lrmodel.summary.tValues,
        "p-Values": lrmodel.summary.pValues
    }))
    print()
    print("Dependent variable:", lrmodel.summary.labelCol)
    print("Number of observations:", lrmodel.summary.numInstances)
    print("RMSE:       {:.6f}".format(lrmodel.summary.rootMeanSquaredError))
    print("R2:         {:.6f}".format(lrmodel.summary.r2))
    print("Iterations:", lrmodel.summary.totalIterations)
    
