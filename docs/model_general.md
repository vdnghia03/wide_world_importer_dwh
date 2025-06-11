# Quy Tắc Xây Dựng Mô Hình Dữ Liệu (Model)

---

## 1. Quy Tắc Chung Cho Model

- Phải **đổi tên** theo chuẩn **Naming Convention**.
- Phải **chuyển kiểu dữ liệu (cast)** phù hợp.
- Phải **tách lớp CTE** theo mục đích xử lý rõ ràng. Một số lớp CTE tiêu biểu:
  - `source`
  - `rename_column`
  - `cast_type`
  - `handle_null`
  - `add_undefined_record`
  - `calculate_measure`
- Khi **flatten** dữ liệu từ **dimension** hoặc từ **fact header/fact line**, phải dùng **`LEFT JOIN`**.
- Khi sử dụng câu lệnh **JOIN**, phải theo định dạng:  
  `<table>.<column>`

---

## 2. Bảng Fact

- Bảng **fact header** và **fact line** được **gộp chung** làm một bảng.
- Phải bao gồm **tất cả các calculated measure** mà user sẽ sử dụng.
- Có liên kết đến các **dimension tương ứng** qua **foreign key**.
- Các **foreign key phải được khử null**.
- **Thứ tự các cột** trong bảng fact:
  1. **Primary Key**
  2. **Description**
  3. **Foreign Key**
  4. **Date/Datetime**
  5. **Fact (measure)**

---

## 3. Bảng Dimension

- Phải **flatten tất cả các dimension** mà người dùng cần dùng.
- Tất cả các cột **phải được khử null**.
- Phải **thêm dòng `Undefined`** cho các trường hợp không xác định.
- **Thứ tự các cột** trong bảng dimension:
  1. **Primary Key**
  2. **Name/Description**
  3. **Attributes**
  4. **Fact**
  5. **Date/Datetime**
  6. **Foreign key & thông tin từ các bảng dimension được flatten**
     - Các bảng dimension được flatten sẽ có thứ tự tương tự
     - Các bảng dimension được flatten được **sắp xếp theo tần suất sử dụng**:  
       **dùng nhiều → đặt trước**

---

## 4. Dữ Liệu

- Dữ liệu **null hoặc trống** phải được đưa về `"Undefined"`.
- Dữ liệu kiểu **boolean** phải có định dạng như sau:
  - Với giá trị **khẳng định**: `"Attribute"`
  - Với giá trị **phủ định**: `"Not Attribute"`  
    *(lưu ý viết hoa đúng ký tự đầu từ)*

---

