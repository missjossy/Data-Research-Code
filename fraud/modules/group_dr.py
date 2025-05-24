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

def add_multi_chart(output1, title, dup_col, var_col):
    # Create the plot

    fig, ax1 = plt.subplots(figsize=(14, 8))
    ax2 = ax1.twinx()

    # Define a colormap
    colors = plt.cm.tab10(np.linspace(0, 1, len(output1[var_col].unique())))
    color_dict = {val: colors[i] for i, val in enumerate(sorted(output1[var_col].unique()))}

    # Plot bars and lines for each value in var_col
    for i, val in enumerate(sorted(output1[var_col].unique())):
        subset = output1[output1[var_col] == val]
        
        # Add slight offset to x positions for each value to avoid overlap
        width = 0.15
        offset = (i - len(output1[var_col].unique())/2) * width
        
        # Plot n_disbursed as bars
        bars = ax1.bar(subset[dup_col] + offset, subset['n_disbursed'], 
                    width=width, color=color_dict[val], alpha=0.7,
                    label=f'n_disbursed ({var_col}={val})')
        
        # Plot dr as lines with markers
        line = ax2.plot(subset[dup_col] + offset, subset['dr'], 
                    linestyle='-', marker='o', color=color_dict[val],
                    alpha=1.0, linewidth=2,
                    label=f'dr ({var_col}={val})')
        for bar in bars: 
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2, height, f'{height}', ha='center', va='bottom', fontsize=10, color='black')


    # Set labels and title
    ax1.set_xlabel(dup_col, fontsize=12)
    ax1.set_ylabel('n_disbursed', fontsize=12, color='black')
    ax2.set_ylabel('dr (Default Rate)', fontsize=12, color='black')

    # Combine legends from both axes
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left', fontsize=10)

    # Set title
    plt.title(title, fontsize=14)

    # Adjust layout and display
    plt.tight_layout()
    plt.grid(True, alpha=0.3)
    plt.show()