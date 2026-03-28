# GitHub 上传与 Windows 部署

## 1. 首次拉取仓库

```powershell
git clone <YOUR_GITHUB_REPO_URL> quant-lab
cd quant-lab
```

## 2. 准备 Python 环境

建议安装：

- Git for Windows
- Python 3.12

创建并激活虚拟环境：

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -e ".[dev]"
```

## 3. 准备本地配置

这些文件不会上传到 GitHub，需要在每台机器本地生成：

```powershell
Copy-Item .env.example .env
Copy-Item config\settings.example.yaml config\settings.yaml
```

然后按本机环境填写：

- `.env`
- `config\settings.yaml`

至少检查：

- OKX API 配置
- Telegram / 邮件告警配置
- 是否允许下单
- 本地数据库路径和端口

## 4. 启动项目

启动本地服务和中文客户端：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\StartQuantLabChineseClient.ps1
```

如果只启动服务：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\StartQuantLab.ps1 -Restart
```

默认访问地址：

- `http://127.0.0.1:18080/`
- `http://127.0.0.1:18080/client`

## 5. 更新代码

后续在另一台电脑同步最新代码：

```powershell
git pull
.\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
```

如果 `settings.yaml` 或 `.env` 已经本地维护，不要用仓库里的示例文件覆盖。
