# Google Sheet 多维度数据分析工具

基于 Streamlit + gspread 的交互式 Google Sheet 数据分析网页应用。

## 功能

- **数据筛选** — 侧边栏自动识别分类列（多选）和数值列（范围滑块）
- **数据概览** — 行数、列数、统计摘要
- **分组聚合** — 支持 sum/mean/count/min/max/median，柱状图/饼图/折线图
- **透视表** — 行列交叉分析 + 热力图
- **散点图** — 双数值列散点，支持按分类着色
- **分布分析** — 直方图 + 箱线图
- **相关性** — 数值列相关系数矩阵热力图

## 前置准备

### 1. 创建 Google Cloud 服务账号

1. 访问 [Google Cloud Console](https://console.cloud.google.com/)
2. 创建项目（或选择已有项目）
3. 启用 **Google Sheets API** 和 **Google Drive API**
4. 进入「API 和服务 → 凭据」，创建「服务账号」
5. 为服务账号创建密钥，选择 JSON 格式，下载保存

### 2. 放置密钥文件

将下载的 JSON 密钥文件重命名为 `credentials.json`，放到本项目根目录下。

### 3. 共享 Google Sheet

将你的 Google Sheet 共享给 `credentials.json` 中 `client_email` 字段对应的邮箱地址（查看者权限即可）。

## 安装与运行

```bash
# 安装依赖
pip install -r requirements.txt

# 启动应用
streamlit run app.py
```

浏览器会自动打开 http://localhost:8501，粘贴 Google Sheet 链接即可开始分析。
