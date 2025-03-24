import pandas as pd
import numpy as np
import matplotlib. pyplot as plt
import seaborn as sns

def add_bplots(data ):
    dup_address = data.copy()
    dup_address['dr'] = dup_address['DEFAULTED']/dup_address['DISBURSED']
    sns.boxplot(x= 'nsurveys', y= 'dr', data = dup_address)
    plt.title('DR Distribution by Address Duplication Count')
    plt.plot()

def add_chart(data, title):
    ## summary = data[data['DISBURSED'] == 1]
    summary = data[data['COUNT'] > 1]
    summary = summary.groupby(['COUNT']).agg(
        nloans = ('DISBURSED', sum),
        ndefaults = ('DR1', sum)
    )
    summary['dr1'] = summary['ndefaults']/summary['nloans']
    summary = summary.reset_index()
    fig, ax1 = plt.subplots(figsize=(10, 4))


    bars = ax1.bar(summary['COUNT'], summary['nloans'], color='skyblue', label='Count')
    ax1.set_xlabel('Monthly Address Duplication')
    ax1.set_ylabel('nLoans', color='black')
    ax1.tick_params(axis='y', labelcolor='black')

    ax1.set_xticks(summary['COUNT'])
    ax1.set_xticklabels(summary['COUNT'], rotation=90)
    # Create a second y-axis
    ax2 = ax1.twinx()
    ax2.plot(summary['COUNT'],summary['dr1'], color='red', marker='o', linestyle='-', linewidth=2, label='Rate')
    ax2.set_ylabel('Default Rate', color='black')
    ax2.tick_params(axis='y', labelcolor='black')

    for bar in bars: 
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2, height, f'{height}', ha='center', va='bottom', fontsize=10, color='black')


    plt.title(title)
    return summary ##plt.show()
