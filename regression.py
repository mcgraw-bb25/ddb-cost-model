# no shbang here, run on python3.7.0
# python3.7 regression.py > regression.log

import pandas as pd
import statsmodels.formula.api as smf

df = pd.read_csv("master_spreadsheet.csv")

# Since the dependent variable is the relative performance
# compared to PostgreSQL, we discard the PostgreSQL data
# from the data set.
df = df[df.Database != 'PostgreSQL']


# Initially these variables envisioned more variability 
# in the total network distance and multi node count.
# Due to limitations in both budget and time a smaller
# set of factors was explored.  Since there is little variability
# in the values of MultiNodeCount (always 3 when not 0)
# and only two batches of cross data centers, these
# values end up having high multicollinearity.  In further
# research they could be explored with greater variation.
print("RuntimeFactor -- Draft Version")
ols = """RuntimeFactor ~ Workers \
                        + IsMultiDC \
                        + MultiNodeCount \
                        + TotalNetworkDistance"""

est = smf.ols(formula=ols, data=df).fit()
print(est.summary())

# Since the experiment wasn't able to run with enough
# iterations, as discussed above, we have reverted to
# relying entirely on dummy variables to capture 
# the explanatory effects of the various database
# deployment designs.  
print("RuntimeFactorAllDummy -- Published Version")
ols = """RuntimeFactor ~ Workers \
                        + IsMultiNode \
                        + IsMultiDC \
                        + IsMultiContinent"""

est = smf.ols(formula=ols, data=df).fit()
print(est.summary())