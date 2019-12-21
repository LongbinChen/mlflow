import argparse
import json
from operators.base import Operator, InputDataType, OutputDataType, StringParam
import logging

from pandas import read_csv
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
from sklearn.naive_bayes import GaussianNB
from sklearn.svm import SVC

from sklearn.model_selection import cross_val_score
from sklearn.model_selection import StratifiedKFold

# Load dataset

logger = logging.getLogger("SKClassifier")


class SKLearnClassifierOperator(Operator):
    def __init__(self,
                input_data:InputDataType,
                output_data:OutputDataType,
                label_column:StringParam):
        self.input_data = str(InputDataType(input_data))
        self.output_data = str(OutputDataType(output_data))
        self.label_column = str(label_column)

    def execute(self):
        df = read_csv(self.input_data, sep="\t")
        # Split-out validation dataset
        if self.label_column not in df.columns:
            logger.error("Can not find the label column with name %s from %s" % (self.label_column, ",".join(df.columns)))
        X = df.loc[:, df.columns != self.label_column].values
        y = df[self.label_column].values

        models = list()
        models.append(('LR', LogisticRegression(solver='liblinear', multi_class='ovr')))
        models.append(('LDA', LinearDiscriminantAnalysis()))
        models.append(('KNN', KNeighborsClassifier()))
        models.append(('CART', DecisionTreeClassifier()))
        models.append(('NB', GaussianNB()))
        models.append(('SVM', SVC(gamma='auto')))

        # evaluate each model in turn
        results = []
        names = []

        with open(self.output_data, "w") as fout:
            fout.write("classifier\terror_mean\terror_std\n")
            for name, model in models:
                kfold = StratifiedKFold(n_splits=10)
                cv_results = cross_val_score(model, X, y, cv=kfold, scoring='accuracy')
                results.append(cv_results)
                names.append(name)
                fout.write('%s\t%f\t%f\n' % (name, cv_results.mean(), cv_results.std()))
