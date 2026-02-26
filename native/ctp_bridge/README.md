# ctp_bridge_native（简化说明）

`ctp_bridge_native` 是 `trader` 的本地 C++ 扩展，用于通过 pybind11 直连 CTP Thost API。

## 目录结构

- `CMakeLists.txt`：构建入口
- `src/py_module.cpp`：`CtpClient` 导出与回调桥接
- `api/win`、`api/linux`：Thost 头文件与库

## 构建

在 `native/ctp_bridge` 目录下执行 CMake 构建。

当前构建设置：

- C++ 标准：`C++23`
- Windows + MSVC：启用 `/std:c++latest` 与 `/utf-8`

### Windows（MSVC）最小命令

```powershell
cd native/ctp_bridge
cmake -S . -B build -G "Visual Studio 17 2022" -A x64
cmake --build build --config Release
```

### Linux（GCC/Clang）最小命令

```bash
cd native/ctp_bridge
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```

### 动态库准备（按平台，必须）

除了头文件，还需要把 CTP 动态库放到对应平台目录：

- Linux：`native/ctp_bridge/api/linux/`
  - `thosttraderapi_se.so`
  - `thostmduserapi_se.so`
  - `DataCollect.so` 或 `LinuxDataCollect.so`
- Windows：`native/ctp_bridge/api/win/`
  - 对应的 Trader/Md/DataCollect 运行库（通常为 `.dll`，并与 `.lib` 配套）

> 注意：前两个交易/行情库用于编译与链接 Python 扩展模块；`DataCollect` 库为行情链路运行时自动调用依赖，也必须放在对应目录。

编译成功后，通常模块输出在：

- Windows：`native/ctp_bridge/build/Release`
- Linux：`native/ctp_bridge/build`

## 配置

在项目根目录 `config.yaml` 的 `ctp_native` 章节确保：

- `gateway = pybind`
- `module = ctp_bridge_native`
- `module_path = <构建输出目录>`（例如 `native/ctp_bridge/build/Release`）

### `module_path` 推荐写法（按平台）

在 `config.yaml` 的 `ctp_native` 章节中可直接写为：

- Windows（Release）

`module_path: native/ctp_bridge/build/Release`

- Windows（Debug）

`module_path: native/ctp_bridge/build/Debug`

- Linux（单配置生成器）

`module_path: native/ctp_bridge/build`

建议优先使用 Release；若你本地是 Debug 构建，请同步把 `module_path` 指到 `Debug` 目录。

## 快速验证

回到项目根目录执行：

- `python test/test_ctp.py`

Linux 建议先做一次依赖检查（确认模块已正确链接 Thost 动态库）：

```bash
ldd ctp_bridge_native.cpython-314-x86_64-linux-gnu.so
```

输出中应能看到：

- `thosttraderapi_se.so => .../native/ctp_bridge/api/linux/thosttraderapi_se.so`
- `thostmduserapi_se.so => .../native/ctp_bridge/api/linux/thostmduserapi_se.so`

按当前配置方案，只需在 `config.yaml` 里设置好 `ctp_native.module_path`，无需把扩展模块复制到项目根目录。

若需要企业微信日志推送，请在 `config.yaml` 的 `weixin` 章节补齐 `CorpID`、`Secret`（以及可选 `AgentID`、`ToUser`）。
当前 `trader` 已内置该推送能力，无需再单独运行 `flower`。

可选环境变量：

- `CTP_TEST_WARMUP_SECONDS`：登录预热秒数
- `CTP_TEST_INSTRUMENT`：优先测试合约（若无效会自动回退）

## 常见问题（Linux）

- `ModuleNotFoundError: No module named 'ctp_bridge_native'`
  - 原因：`ctp_native.module_path` 未指向构建目录。
  - 处理：检查 `config.yaml` 中 `ctp_native.module_path` 是否正确。

- `ImportError: undefined symbol: CThostFtdc...CreateFtdc...`
  - 原因：模块未正确链接/加载 Thost 动态库。
  - 处理：确认 `api/linux` 下两个 `thost*.so` 文件存在，重新执行 CMake 构建，并用 `ldd` 验证依赖是否解析成功。
