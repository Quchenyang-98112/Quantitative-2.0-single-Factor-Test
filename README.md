# ShiXi-NuoFengHui

## 项目简介

本项目是一个面向股票量化研究的单因子选股/分层回测实践项目，围绕**因子构建—标准化处理—IC 检验—分层回测—组合优化**这一完整研究链路展开。

项目目标是基于给定的日频股票数据，完成从原始数据预处理到因子有效性评估、从分组收益检验到组合增强优化的全流程实现，并形成可复现实验结果。

从当前代码结构来看，项目以 Python 为主要开发语言，采用模块化脚本组织方式，适合课程实践、量化研究训练、策略原型验证与研究报告撰写。

---

## 项目目标

本项目主要完成以下任务：

1. 对原始股票日频数据进行预处理与字段检查。
2. 构建单因子或基础量化因子。
3. 对因子进行标准化处理，并为后续中性化扩展预留接口。
4. 使用 IC（Information Coefficient）与 Rank IC 对因子有效性进行检验。
5. 基于分层回测评估因子的选股能力与收益区分能力。
6. 在基础因子框架上进一步进行收益增强或组合优化分析。

---

## 项目结构

```text
ShiXi-NuoFengHui/
├─ .gitignore
├─ DY-IND-CHG_STATUS_分析报告.txt
├─ DY-IND-CHG_STATUS_完整取值报告.txt
├─ analyze_dy_ind_chg_status_detailed.py
├─ analyze_dy_ind_chg_status_qualitative.py
├─ analyze_dy_ind_chg_status_relationships.py
├─ check_columns.py
├─ check_daily_data_columns.py
├─ extract_dy_ind_chg_status.py
├─ filter_final_results.py
├─ fix_main.py
├─ Task_require/
│  └─ 单因子检验.md
└─ src/
   ├─ 01_data_prepare.py
   ├─ 02_factor_build.py
   ├─ 03_standardize_neutralize.py
   ├─ 04_ic_test.py
   ├─ 05_layer_backtest.py
   ├─ 06_bonus_opt.py
   └─ main.py
```

---

## 各模块说明

### 1. 数据检查与辅助分析脚本

* `check_columns.py`
  用于检查原始数据表字段，帮助明确列名、字段含义及后续处理所需变量。

* `check_daily_data_columns.py`
  针对日频数据进一步核验字段结构与可用性。

* `extract_dy_ind_chg_status.py`
  提取并分析 `DY-IND-CHG_STATUS` 字段，为涨跌停、交易状态识别等规则构建提供依据。

* `analyze_dy_ind_chg_status_detailed.py`
  对交易状态字段进行详细统计分析。

* `analyze_dy_ind_chg_status_qualitative.py`
  对状态取值进行定性解释与分类研究。

* `analyze_dy_ind_chg_status_relationships.py`
  研究交易状态字段与价格、成交、涨跌停等变量之间的关系。

* `filter_final_results.py`
  用于筛选、整理或汇总最终结果。

* `fix_main.py`
  用于修复或调整主流程调用逻辑。

---

### 2. 核心研究流程脚本（`src/`）

#### `01_data_prepare.py`

数据准备模块。主要用于：

* 读取原始股票数据；
* 清洗和整理关键字段；
* 合并停牌、ST、行业、市值等辅助信息；
* 生成后续因子构建与回测使用的基础面板数据。

#### `02_factor_build.py`

因子构建模块。主要用于：

* 基于价格、收益率、成交量等信息构建原始因子；
* 明确因子值与未来收益之间的对齐关系；
* 输出因子面板数据。

#### `03_standardize_neutralize.py`

标准化与中性化模块。主要用于：

* 对原始因子做去极值、标准化等处理；
* 为市值中性化、行业中性化预留扩展路径；
* 生成更适合检验和回测的标准因子。

#### `04_ic_test.py`

IC 检验模块。主要用于：

* 计算 Pearson IC；
* 计算 Rank IC；
* 评估因子与未来收益的相关性方向与稳定性；
* 输出 IC 汇总结果及相关可视化材料。

#### `05_layer_backtest.py`

分层回测模块。主要用于：

* 按因子值对股票进行横截面分组；
* 计算不同分组组合的收益表现；
* 检验高分组与低分组之间是否存在稳定收益差异；
* 输出分层收益表现、累计净值与绩效指标。

#### `06_bonus_opt.py`

增强/优化模块。主要用于：

* 在已有单因子研究基础上进行进一步优化；
* 可能包括参数增强、组合加权、收益增强或多指标融合；
* 为最终策略表达和实证结果提升提供支持。

#### `main.py`

主流程调度脚本。主要用于：

* 串联整个研究流程；
* 按既定顺序执行数据准备、因子构建、标准化、IC 检验、分层回测和优化分析。

---

## 研究流程

本项目的标准研究流程如下：

```text
原始数据读取
    ↓
数据清洗与字段核验
    ↓
基础面板构建
    ↓
因子构建
    ↓
标准化 / 中性化处理
    ↓
IC / Rank IC 检验
    ↓
分层回测
    ↓
收益增强 / 组合优化
    ↓
结果筛选与总结
```

---

## 数据说明

本项目依赖股票日频数据及相关辅助数据，通常包括以下信息：

* 复权收盘价
* 复权开盘价
* 复权最高价
* 复权最低价
* 成交量 / 成交额
* 总市值 / 流通市值
* 停牌标记
* ST 标记
* 行业分类
* 交易状态字段（如 `DY-IND-CHG_STATUS`）

由于原始数据体量较大，且包含较大的数据文件，本仓库默认**不上传原始数据与大型结果文件**，仅保留代码、说明文档和核心分析脚本。

如需完整复现，请在本地准备相应数据文件，并根据脚本中的路径设置进行调整。

---

## 运行环境

建议环境：

* Python 3.10+
* Windows 10/11
* Anaconda / Miniconda
* VSCode 或 PyCharm

常用依赖可能包括：

* pandas
* numpy
* scipy
* matplotlib
* scikit-learn
* pyarrow

可根据实际代码进一步补充 `requirements.txt`。

---

## 运行方式

### 方法一：按模块依次运行

可按以下顺序逐步执行：

```bash
python src/01_data_prepare.py
python src/02_factor_build.py
python src/03_standardize_neutralize.py
python src/04_ic_test.py
python src/05_layer_backtest.py
python src/06_bonus_opt.py
```

### 方法二：直接运行主程序

```bash
python src/main.py
```

> 实际执行前，请根据本地数据路径、输入文件位置和输出目录设置，对脚本中的路径进行检查与修改。

---

## 输出结果

项目可能输出以下类型的结果：

* 因子面板数据
* 标准化后的因子文件
* IC / Rank IC 汇总结果
* 分层回测绩效指标
* 累计收益曲线
* 分组收益统计表
* 优化后的组合结果或增强结果

这些结果通常用于：

* 判断因子是否有效；
* 判断因子方向是否正确；
* 判断因子是否具备稳定的收益分层能力；
* 评估策略是否具有进一步优化空间。

---

## 项目特点

* 采用模块化设计，结构清晰；
* 覆盖从数据处理到回测评估的完整量化研究链条；
* 包含针对交易状态字段的专项分析脚本；
* 适合用于课程实践、实习项目、因子研究训练与研究报告撰写；
* 便于后续扩展为多因子模型、行业中性化框架或组合优化系统。

---

## 注意事项

1. 本仓库未包含完整原始数据与大体量结果文件。
2. 若运行报错，请优先检查：

   * 数据路径是否正确；
   * 字段名是否与脚本一致；
   * 依赖库是否完整安装；
   * 编码格式是否统一。
3. Windows 环境下 Git 可能提示 `LF will be replaced by CRLF`，这通常是换行符提醒，不影响项目使用。

---

## 后续可扩展方向

* 增加 `requirements.txt` 以固定依赖环境；
* 增加 `config/` 配置目录，统一管理路径和参数；
* 增加 `notebooks/` 用于探索性分析；
* 增加 `sample_data/` 提供小样例数据；
* 增加 `README` 中的结果图展示与核心结论摘要；
* 将单因子研究扩展为多因子组合模型。

---

## 说明

本仓库当前主要保留量化研究的核心代码与文档说明，用于展示项目流程、分析逻辑与方法实现。若需完整复现实验，请结合本地原始数据与结果文件运行。
