# NAMIMG CONVENTION
---

## Tên bảng

Tên bảng bắt đầu bằng fact_  hoặc dim_
Tên bảng là số ít

- Bảng Fact:
    + fact_<table>
    + Ví dụ đúng: fact_sales_order_line
    + Ví dụ sai: fact_sales_order_line**s**
  
- Bảng Dim:
    + dim_<table>
    + Ví dụ đúng: dim_product
    + Ví dụ sai: dim_product**s**



## Tên cột

Tên cột primary key phải tương ứng với tên bảng

- Nếu tên bảng là fact_<table> thì tên cột primary key là <table>_key
  + Ví dụ: bảng fact_sales_order_line thì cột primary key là sales_order_line_key

- Nếu tên bảng là dim_<table> thì tên cột primary key là <table>_key
  + Ví dụ: bảng dim_product thì cột primary key là product_key