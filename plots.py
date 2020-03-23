import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("master_spreadsheet.csv")

df.boxplot(by='BatchDescription', 
           column=['RuntimeFactor'], 
           grid=True,
           fontsize=8,
           vert=False)

plt.title("")
plt.suptitle("")
plt.tight_layout()
plt.show(block=True)