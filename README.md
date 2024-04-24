
# YouTube Data Harvesting and Warehousing

This project aims to extract YouTube Data - Channel Information, Video Information, Comments Information for the input Channel ID . The Extracted data is then loaded into an SQL Database which is used for querying.

This Project is implemented on Streamlit UI.

The Scripting Language used is Python.

Data is extracted from Youtube through API

Data transformation is achieved using pandas.

Data is loaded into MySQL local database.


## API Reference

#### Get all items

```http
  GET /api/items
```

| Parameter | Type     | Description                |
| :-------- | :------- | :------------------------- |
| `api_key` | `string` | 'AIzaSyA_EG9ouNelnuI6W597WiOGVwSCRCg5sLA'|

#### Get item

```http
  GET /api/items/${id}
```

| Parameter | Type     | Description                       |
| :-------- | :------- | :-------------------------------- |
| `channel_id`      | `string` | UCJ8f7P3z3q6ztGZx1mDcwZw |




## Authors

- [KrithigaSelvam](https://github.com/KrithigaSelvam/Krithiga)


## Deployment

To deploy this project run

```
streamlit run YouTube_Data_Harv_Ware.py
```


## 🚀 About Me
I'm a Data Science Enthusiast.

I am currently dabbling in Python with Pandas, SQL, Exploratory Data Analysis and Visualisation of Data using Matplotlib/seaborn/plotly


# Hi, I'm Krithiga! 👋



## Support

For support, email rithi.3418@gmail.com


## Tech Stack

**Client:** Streamlit

**Script:** Python, Pandas

**Server:** Youtube API

**Database:** MySQL


## Appendix

Python Packages to be pre-installed

1.pandas

2.mysql-connector-Python

3.sqlalchemy

4.google-api-python-client

5.streamlit
