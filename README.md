#

## Prerequisites
- Python 3.8
- Java: openjdk 17.0.4 2022-07-19
- some python packages

## Get Running

- Create a virtual environment for Python packages.
  python3 -m venv env
- Activate it.
  source env/bin/activate
- Populate it with needed packages.
  python3 -m pip install -r requirements.txt 
- Run the Python script.
  python3  facts_done.py

## HOW THIS WORKS: 6 tricks

### Trick 1: windowing functions
   SQL windowing functions are different in at least two ways from the 
    usual aggrate(variable)/group-by:
 - The group is not collapsed into a single row with aggregated values, 
    it remains with all its rows.
 - Aggregation isn't over the whole partition, but row-by-row. A sum's 
    value increases as you go down the partition's rows, increasing with each row.

### Trick 2: Use the LAG function to get the previous row and calculate the number 
    of days between these events: column 'gap'.

### Trick 3: create a flag, 'within_run', with values 0 or 1, to tell when 
    the gap was only a single day.

### Trick 4: create the opposite flag, 'between_run', with values 0 or 1, 
    to tell when a consecutive run was broken because the gap was more 
    than a single day.

### Trick 5: sum the between_run to create the column run_number

### Trick 6: use run_number to change the parition to break on runs as well as persons,
    then sum the within_run flags to get the step_number

With that you can select for person_id and event_date where the step_number is 9.
That tells the last day in a run of 10 consecutive days.


TESTING:
Note that the data has shorter runs, but also a run of 10 days where the 
vaccine_administration is always 0.

'''
