#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <filesystem>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
#include "ThostFtdcMdApi.h"
#include "ThostFtdcTraderApi.h"
namespace py = pybind11;
namespace {
void copy_cstr(char* dst, size_t dst_size, const std::string& value) {
    if (dst == nullptr || dst_size == 0) {
        return;
    }
    std::memset(dst, 0, dst_size);
    if (!value.empty()) {
        std::strncpy(dst, value.c_str(), dst_size - 1);
    }
}
std::string get_str(const py::dict& d, const char* key, const std::string& fallback = "") {
    if (!d.contains(py::str(key))) {
        return fallback;
    }
    return py::str(d[py::str(key)]).cast<std::string>();
}
int get_int(const py::dict& d, const char* key, int fallback = 0) {
    if (!d.contains(py::str(key))) {
        return fallback;
    }
    return py::int_(d[py::str(key)]).cast<int>();
}
double get_double(const py::dict& d, const char* key, double fallback = 0.0) {
    if (!d.contains(py::str(key))) {
        return fallback;
    }
    return py::float_(d[py::str(key)]).cast<double>();
}
py::str decode_text(const char* value) {
    if (value == nullptr) {
        return py::str("");
    }
    const auto len = std::strlen(value);
    if (len == 0) {
        return py::str("");
    }
    PyObject* obj = PyUnicode_Decode(value, static_cast<Py_ssize_t>(len), "utf-8", "strict");
    if (!obj) {
        PyErr_Clear();
        obj = PyUnicode_Decode(value, static_cast<Py_ssize_t>(len), "gb18030", "strict");
    }
    if (!obj) {
        PyErr_Clear();
        obj = PyUnicode_Decode(value, static_cast<Py_ssize_t>(len), "gbk", "strict");
    }
    if (!obj) {
        PyErr_Clear();
        obj = PyUnicode_Decode(value, static_cast<Py_ssize_t>(len), "utf-8", "replace");
    }
    if (!obj) {
        PyErr_Clear();
        obj = PyUnicode_Decode(value, static_cast<Py_ssize_t>(len), "gb18030", "replace");
    }
    if (!obj) {
        PyErr_Clear();
        obj = PyUnicode_Decode(value, static_cast<Py_ssize_t>(len), "gbk", "replace");
    }
    if (!obj) {
        PyErr_Clear();
        obj = PyUnicode_DecodeLatin1(value, static_cast<Py_ssize_t>(len), nullptr);
    }
    if (!obj) {
        PyErr_Clear();
        return py::str("");
    }
    return py::reinterpret_steal<py::str>(obj);
}
} // namespace
class CtpClient;
class TraderSpi final : public CThostFtdcTraderSpi {
  public:
    explicit TraderSpi(CtpClient* owner) : owner_(owner) {}
    void OnFrontConnected() override;
    void OnFrontDisconnected(int nReason) override;
    void OnRspAuthenticate(CThostFtdcRspAuthenticateField* pRspAuthenticateField, CThostFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast) override;
    void OnRspUserLogin(CThostFtdcRspUserLoginField* pRspUserLogin, CThostFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast) override;
    void OnRspError(CThostFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast) override;
    void OnRspQryTradingAccount(CThostFtdcTradingAccountField* pStruct, CThostFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast) override;
    void OnRspQryInvestorPositionDetail(CThostFtdcInvestorPositionDetailField* pStruct, CThostFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast) override;
    void OnRspQryInstrument(CThostFtdcInstrumentField* pStruct, CThostFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast) override;
    void OnRspQryInstrumentCommissionRate(CThostFtdcInstrumentCommissionRateField* pStruct, CThostFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast) override;
    void OnRspQryInstrumentMarginRate(CThostFtdcInstrumentMarginRateField* pStruct, CThostFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast) override;
    void OnRspQryOrder(CThostFtdcOrderField* pStruct, CThostFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast) override;
    void OnRspQryTrade(CThostFtdcTradeField* pStruct, CThostFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast) override;
    void OnRspSettlementInfoConfirm(CThostFtdcSettlementInfoConfirmField* pSettlementInfoConfirm, CThostFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast) override;
    void OnRspOrderInsert(CThostFtdcInputOrderField* pStruct, CThostFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast) override;
    void OnRspOrderAction(CThostFtdcInputOrderActionField* pStruct, CThostFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast) override;
    void OnRtnOrder(CThostFtdcOrderField* pStruct) override;
    void OnRtnTrade(CThostFtdcTradeField* pStruct) override;

  private:
    CtpClient* owner_;
};
class MdSpi final : public CThostFtdcMdSpi {
  public:
    explicit MdSpi(CtpClient* owner) : owner_(owner) {}
    void OnFrontConnected() override;
    void OnFrontDisconnected(int nReason) override;
    void OnRspUserLogin(CThostFtdcRspUserLoginField* pRspUserLogin, CThostFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast) override;
    void OnRspError(CThostFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast) override;
    void OnRspSubMarketData(CThostFtdcSpecificInstrumentField* pSpecificInstrument, CThostFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast) override;
    void OnRspUnSubMarketData(CThostFtdcSpecificInstrumentField* pSpecificInstrument, CThostFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast) override;
    void OnRtnDepthMarketData(CThostFtdcDepthMarketDataField* pData) override;

  private:
    CtpClient* owner_;
};
class CtpClient {
  public:
    CtpClient() = default;
    ~CtpClient() { stop(); }
    void configure(const py::dict& cfg) {
        std::lock_guard<std::mutex> lk(mu_);
        trade_front_ = get_str(cfg, "trade_front", trade_front_);
        market_front_ = get_str(cfg, "market_front", market_front_);
        broker_id_ = get_str(cfg, "broker_id", broker_id_);
        investor_id_ = get_str(cfg, "investor_id", investor_id_);
        password_ = get_str(cfg, "password", password_);
        appid_ = get_str(cfg, "appid", appid_);
        authcode_ = get_str(cfg, "authcode", authcode_);
        userinfo_ = get_str(cfg, "userinfo", userinfo_);
        ip_ = get_str(cfg, "ip", ip_);
        mac_ = get_str(cfg, "mac", mac_);
        flow_path_ = get_str(cfg, "flow_path", flow_path_);
        request_timeout_ms_ = get_int(cfg, "request_timeout_ms", request_timeout_ms_);
    }
    void start() {
        std::lock_guard<std::mutex> lk(mu_);
        if (started_) {
            return;
        }
        if (trade_front_.empty() || market_front_.empty() || broker_id_.empty() || investor_id_.empty() || password_.empty() || appid_.empty() || authcode_.empty() || userinfo_.empty()) {
            throw std::runtime_error("CTP_NATIVE config missing required fields: "
                                     "trade_front/market_front/broker_id/investor_id/"
                                     "password/appid/authcode/userinfo");
        }
        std::filesystem::create_directories(flow_path_ + "/trade");
        std::filesystem::create_directories(flow_path_ + "/md");
        trader_api_ = CThostFtdcTraderApi::CreateFtdcTraderApi((flow_path_ + "/trade").c_str());
        md_api_ = CThostFtdcMdApi::CreateFtdcMdApi((flow_path_ + "/md").c_str());
        if (!trader_api_ || !md_api_) {
            throw std::runtime_error("Create CTP API failed");
        }
        trader_spi_ = new TraderSpi(this);
        md_spi_ = new MdSpi(this);
        trader_api_->RegisterSpi(trader_spi_);
        trader_api_->SubscribePublicTopic(THOST_TERT_QUICK);
        trader_api_->SubscribePrivateTopic(THOST_TERT_QUICK);
        trader_api_->RegisterFront((char*)trade_front_.c_str());
        md_api_->RegisterSpi(md_spi_);
        md_api_->RegisterFront((char*)market_front_.c_str());
        started_ = true;
        trade_login_ = false;
        market_login_ = false;
        trader_api_->Init();
        md_api_->Init();
    }
    void stop() {
        std::lock_guard<std::mutex> lk(mu_);
        if (!started_) {
            return;
        }
        started_ = false;
        trade_login_ = false;
        market_login_ = false;
        if (trader_api_) {
            trader_api_->RegisterSpi(nullptr);
            trader_api_->Release();
            trader_api_ = nullptr;
        }
        if (md_api_) {
            md_api_->RegisterSpi(nullptr);
            md_api_->Release();
            md_api_ = nullptr;
        }
        delete trader_spi_;
        trader_spi_ = nullptr;
        delete md_spi_;
        md_spi_ = nullptr;
        pending_.clear();
        request_name_by_id_.clear();
    }
    void set_event_callback(py::object cb) {
        std::lock_guard<std::mutex> lk(mu_);
        event_callback_ = std::move(cb);
    }
    py::list request(const std::string& req_name, const py::object& payload) {
        if (!started_) {
            throw std::runtime_error("CtpClient is not started");
        }
        py::dict d;
        if (py::isinstance<py::dict>(payload)) {
            d = payload.cast<py::dict>();
        }
        int request_id = get_int(d, "RequestID", 0);
        bool is_market_req = (req_name == "SubscribeMarketData" || req_name == "UnSubscribeMarketData");
        bool is_query_req = req_name.rfind("ReqQry", 0) == 0;
        bool is_order_insert = req_name == "ReqOrderInsert";
        if (is_market_req && request_id == 0) {
            request_id = 0; // 保持与现有通道语义一致
        }
        if (is_market_req) {
            wait_login(/*trade=*/false, /*market=*/true);
        } else {
            wait_login(/*trade=*/true, /*market=*/false);
        }
        {
            std::lock_guard<std::mutex> lk(mu_);
            if (!is_order_insert) {
                pending_[request_id] = Pending{};
            }
            if (request_id != 0) {
                request_name_by_id_[request_id] = req_name;
            }
        }
        if (is_query_req) {
            enforce_query_throttle();
        }
        int rc = dispatch_request(req_name, request_id, payload);
        if (rc < 0) {
            py::dict row;
            row["ErrorID"] = rc;
            row["ErrorMsg"] = "Req dispatch failed";
            row["RequestID"] = request_id;
            row["bIsLast"] = true;
            row["empty"] = true;
            std::lock_guard<std::mutex> lk(mu_);
            pending_.erase(request_id);
            request_name_by_id_.erase(request_id);
            py::list rows;
            rows.append(row);
            return rows;
        }
        if (is_order_insert) {
            py::list rows;
            return rows;
        }
        bool timeout = false;
        {
            py::gil_scoped_release release;
            std::unique_lock<std::mutex> lk(mu_);
            const auto timeout_ms = std::chrono::milliseconds(request_timeout_ms_);
            if (!cv_.wait_for(lk, timeout_ms, [&] {
                    auto it = pending_.find(request_id);
                    return it == pending_.end() || it->second.done;
                })) {
                timeout = true;
            }
        }
        py::list out;
        std::lock_guard<std::mutex> lk(mu_);
        auto it = pending_.find(request_id);
        if (timeout || it == pending_.end()) {
            py::dict row;
            row["ErrorID"] = -1001;
            row["ErrorMsg"] = "request timeout";
            row["RequestID"] = request_id;
            row["bIsLast"] = true;
            row["empty"] = true;
            out.append(row);
            pending_.erase(request_id);
            request_name_by_id_.erase(request_id);
            return out;
        }
        for (auto& r : it->second.rows) {
            out.append(r);
        }
        pending_.erase(it);
        request_name_by_id_.erase(request_id);
        return out;
    }
    // ----- SPI helpers -----
    void on_trade_front_connected() {
        py::gil_scoped_acquire gil;
        py::dict evt;
        evt["Reason"] = 0;
        emit_event("OnFrontConnected", evt);
        CThostFtdcReqAuthenticateField req{};
        copy_cstr(req.BrokerID, sizeof(req.BrokerID), broker_id_);
        copy_cstr(req.UserID, sizeof(req.UserID), investor_id_);
        copy_cstr(req.UserProductInfo, sizeof(req.UserProductInfo), userinfo_);
        copy_cstr(req.AuthCode, sizeof(req.AuthCode), authcode_);
        copy_cstr(req.AppID, sizeof(req.AppID), appid_);
        trader_api_->ReqAuthenticate(&req, 1);
    }
    void on_trade_front_disconnected(int nReason) {
        {
            std::lock_guard<std::mutex> lk(mu_);
            trade_login_ = false;
        }
        cv_login_.notify_all();
        py::gil_scoped_acquire gil;
        py::dict evt;
        evt["Reason"] = nReason;
        emit_event("OnFrontDisconnected", evt);
    }
    void on_trade_auth_rsp(CThostFtdcRspInfoField* pRspInfo, int nRequestID) {
        {
            py::gil_scoped_acquire gil;
            py::dict row;
            row["RequestID"] = nRequestID;
            row["bIsLast"] = true;
            if (pRspInfo && pRspInfo->ErrorID != 0) {
                row["ErrorID"] = pRspInfo->ErrorID;
                row["ErrorMsg"] = decode_text(pRspInfo->ErrorMsg);
            } else {
                row["ErrorID"] = 0;
            }
            emit_event("OnRspAuthenticate", row);
        }
        if (pRspInfo && pRspInfo->ErrorID != 0) {
            on_rsp_error(nRequestID, pRspInfo->ErrorID, pRspInfo->ErrorMsg);
            return;
        }
        send_trade_login(nRequestID + 1);
    }
    void on_trade_login_rsp(CThostFtdcRspUserLoginField* pRspUserLogin, CThostFtdcRspInfoField* pRspInfo, int nRequestID) {
        py::gil_scoped_acquire gil;
        py::dict row;
        if (pRspInfo && pRspInfo->ErrorID != 0) {
            row["ErrorID"] = pRspInfo->ErrorID;
            row["ErrorMsg"] = decode_text(pRspInfo->ErrorMsg);
            row["RequestID"] = nRequestID;
            row["bIsLast"] = true;
            emit_event("OnRspUserLogin", row);
            return;
        }
        {
            std::lock_guard<std::mutex> lk(mu_);
            trade_login_ = true;
        }
        cv_login_.notify_all();
        row["ErrorID"] = 0;
        row["RequestID"] = nRequestID;
        row["bIsLast"] = true;
        if (pRspUserLogin) {
            row["TradingDay"] = decode_text(pRspUserLogin->TradingDay);
            row["LoginTime"] = decode_text(pRspUserLogin->LoginTime);
            row["BrokerID"] = decode_text(pRspUserLogin->BrokerID);
            row["UserID"] = decode_text(pRspUserLogin->UserID);
            row["SystemName"] = decode_text(pRspUserLogin->SystemName);
            row["FrontID"] = pRspUserLogin->FrontID;
            row["SessionID"] = pRspUserLogin->SessionID;
            row["MaxOrderRef"] = decode_text(pRspUserLogin->MaxOrderRef);
        }
        emit_event("OnRspUserLogin", row);
        int settlement_req_id = nRequestID + 1;
        int rc = send_settlement_info_confirm(settlement_req_id);
        if (rc < 0) {
            py::dict err;
            err["ErrorID"] = rc;
            err["ErrorMsg"] = "ReqSettlementInfoConfirm dispatch failed";
            err["RequestID"] = settlement_req_id;
            err["bIsLast"] = true;
            emit_event("OnRspSettlementInfoConfirm", err);
        }
    }
    void on_market_front_connected() {
        py::gil_scoped_acquire gil;
        py::dict evt;
        evt["Reason"] = 0;
        emit_event("OnFrontConnectedMd", evt);
        CThostFtdcReqUserLoginField req{};
        copy_cstr(req.BrokerID, sizeof(req.BrokerID), broker_id_);
        copy_cstr(req.UserID, sizeof(req.UserID), investor_id_);
        copy_cstr(req.Password, sizeof(req.Password), password_);
        md_api_->ReqUserLogin(&req, 1);
    }
    void on_market_front_disconnected(int nReason) {
        {
            std::lock_guard<std::mutex> lk(mu_);
            market_login_ = false;
        }
        cv_login_.notify_all();
        py::gil_scoped_acquire gil;
        py::dict evt;
        evt["Reason"] = nReason;
        emit_event("OnFrontDisconnectedMd", evt);
    }
    void on_market_login_rsp(CThostFtdcRspInfoField* pRspInfo) {
        {
            py::gil_scoped_acquire gil;
            py::dict row;
            row["RequestID"] = 1;
            row["bIsLast"] = true;
            if (pRspInfo && pRspInfo->ErrorID != 0) {
                row["ErrorID"] = pRspInfo->ErrorID;
                row["ErrorMsg"] = decode_text(pRspInfo->ErrorMsg);
            } else {
                row["ErrorID"] = 0;
            }
            emit_event("OnRspUserLoginMd", row);
        }
        if (pRspInfo && pRspInfo->ErrorID != 0) {
            return;
        }
        {
            std::lock_guard<std::mutex> lk(mu_);
            market_login_ = true;
        }
        cv_login_.notify_all();
    }
    void on_rsp_settlement_info_confirm(CThostFtdcSettlementInfoConfirmField* pStruct, CThostFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast) {
        py::gil_scoped_acquire gil;
        py::dict row;
        if (pRspInfo && pRspInfo->ErrorID != 0) {
            row["ErrorID"] = pRspInfo->ErrorID;
            row["ErrorMsg"] = decode_text(pRspInfo->ErrorMsg);
            row["empty"] = true;
        } else if (pStruct) {
            row["BrokerID"] = decode_text(pStruct->BrokerID);
            row["InvestorID"] = decode_text(pStruct->InvestorID);
            row["ConfirmDate"] = decode_text(pStruct->ConfirmDate);
            row["ConfirmTime"] = decode_text(pStruct->ConfirmTime);
            row["ErrorID"] = 0;
            row["empty"] = false;
        } else {
            row["ErrorID"] = 0;
            row["empty"] = true;
        }
        row["RequestID"] = nRequestID;
        row["bIsLast"] = bIsLast;
        bool found_pending = false;
        {
            std::lock_guard<std::mutex> lk(mu_);
            auto it = pending_.find(nRequestID);
            if (it != pending_.end()) {
                found_pending = true;
                it->second.rows.push_back(row);
                if (bIsLast) {
                    it->second.done = true;
                    request_name_by_id_.erase(nRequestID);
                    cv_.notify_all();
                }
            }
        }
        if (!found_pending) {
            emit_event("OnRspSettlementInfoConfirm", row);
        }
    }
    void on_rsp_error(int request_id, int error_id, const std::string& error_msg = "") {
        py::gil_scoped_acquire gil;
        py::dict row;
        row["ErrorID"] = error_id;
        row["RequestID"] = request_id;
        if (!error_msg.empty()) {
            row["ErrorMsg"] = decode_text(error_msg.c_str());
        }
        row["bIsLast"] = true;
        row["empty"] = true;
        std::string req_name;
        {
            std::lock_guard<std::mutex> lk(mu_);
            auto req_it = request_name_by_id_.find(request_id);
            if (req_it != request_name_by_id_.end()) {
                req_name = req_it->second;
                request_name_by_id_.erase(req_it);
                row["RequestName"] = req_name;
            }
            auto it = pending_.find(request_id);
            if (it != pending_.end()) {
                it->second.rows.push_back(row);
                it->second.done = true;
                cv_.notify_all();
                return;
            }
        }
        if (req_name == "ReqOrderInsert") {
            emit_event("OnRspOrderInsert", row);
        } else if (req_name == "ReqOrderAction") {
            emit_event("OnRspOrderAction", row);
        } else {
            emit_event("OnRspError", row);
        }
    }
    void on_rsp_trading_account(CThostFtdcTradingAccountField* pStruct, CThostFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast) {
        py::gil_scoped_acquire gil;
        py::dict row;
        if (pRspInfo && pRspInfo->ErrorID != 0) {
            row["ErrorID"] = pRspInfo->ErrorID;
            row["empty"] = true;
        } else if (pStruct) {
            row["BrokerID"] = decode_text(pStruct->BrokerID);
            row["AccountID"] = decode_text(pStruct->AccountID);
            row["Withdraw"] = pStruct->Withdraw;
            row["Deposit"] = pStruct->Deposit;
            row["PreBalance"] = pStruct->PreBalance;
            row["CloseProfit"] = pStruct->CloseProfit;
            row["PositionProfit"] = pStruct->PositionProfit;
            row["Commission"] = pStruct->Commission;
            row["CurrMargin"] = pStruct->CurrMargin;
            row["Available"] = pStruct->Available;
            row["TradingDay"] = decode_text(pStruct->TradingDay);
            row["empty"] = false;
        } else {
            row["empty"] = true;
        }
        row["bIsLast"] = bIsLast;
        std::lock_guard<std::mutex> lk(mu_);
        auto it = pending_.find(nRequestID);
        if (it != pending_.end()) {
            it->second.rows.push_back(row);
            if (bIsLast) {
                it->second.done = true;
                request_name_by_id_.erase(nRequestID);
                cv_.notify_all();
            }
        }
    }
    void on_rsp_investor_position_detail(CThostFtdcInvestorPositionDetailField* pStruct, CThostFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast) {
        py::gil_scoped_acquire gil;
        py::dict row;
        if (pRspInfo && pRspInfo->ErrorID != 0) {
            row["ErrorID"] = pRspInfo->ErrorID;
            row["empty"] = true;
        } else if (pStruct) {
            row["InstrumentID"] = decode_text(pStruct->InstrumentID);
            row["BrokerID"] = decode_text(pStruct->BrokerID);
            row["InvestorID"] = decode_text(pStruct->InvestorID);
            row["Direction"] = pStruct->Direction;
            row["OpenDate"] = decode_text(pStruct->OpenDate);
            row["Volume"] = pStruct->Volume;
            row["OpenPrice"] = pStruct->OpenPrice;
            row["PositionProfitByTrade"] = pStruct->PositionProfitByTrade;
            row["Margin"] = pStruct->Margin;
            row["empty"] = false;
        } else {
            row["empty"] = true;
        }
        row["bIsLast"] = bIsLast;
        std::lock_guard<std::mutex> lk(mu_);
        auto it = pending_.find(nRequestID);
        if (it != pending_.end()) {
            it->second.rows.push_back(row);
            if (bIsLast) {
                it->second.done = true;
                request_name_by_id_.erase(nRequestID);
                cv_.notify_all();
            }
        }
    }
    void on_rsp_instrument(CThostFtdcInstrumentField* pStruct, CThostFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast) {
        py::gil_scoped_acquire gil;
        py::dict row;
        if (pRspInfo && pRspInfo->ErrorID != 0) {
            row["ErrorID"] = pRspInfo->ErrorID;
            row["empty"] = true;
        } else if (pStruct) {
            row["InstrumentID"] = decode_text(pStruct->InstrumentID);
            row["ExchangeID"] = decode_text(pStruct->ExchangeID);
            row["InstrumentName"] = decode_text(pStruct->InstrumentName);
            row["ProductID"] = decode_text(pStruct->ProductID);
            row["CreateDate"] = decode_text(pStruct->CreateDate);
            row["OpenDate"] = decode_text(pStruct->OpenDate);
            row["ExpireDate"] = decode_text(pStruct->ExpireDate);
            row["ProductClass"] = pStruct->ProductClass;
            row["VolumeMultiple"] = pStruct->VolumeMultiple;
            row["PriceTick"] = pStruct->PriceTick;
            row["LongMarginRatio"] = pStruct->LongMarginRatio;
            row["IsTrading"] = pStruct->IsTrading;
            row["empty"] = false;
        } else {
            row["empty"] = true;
        }
        row["bIsLast"] = bIsLast;
        std::lock_guard<std::mutex> lk(mu_);
        auto it = pending_.find(nRequestID);
        if (it != pending_.end()) {
            it->second.rows.push_back(row);
            if (bIsLast) {
                it->second.done = true;
                request_name_by_id_.erase(nRequestID);
                cv_.notify_all();
            }
        }
    }
    void on_rsp_instrument_commission_rate(CThostFtdcInstrumentCommissionRateField* pStruct, CThostFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast) {
        py::gil_scoped_acquire gil;
        py::dict row;
        if (pRspInfo && pRspInfo->ErrorID != 0) {
            row["ErrorID"] = pRspInfo->ErrorID;
            row["empty"] = true;
        } else if (pStruct) {
            row["InstrumentID"] = decode_text(pStruct->InstrumentID);
            row["BrokerID"] = decode_text(pStruct->BrokerID);
            row["InvestorID"] = decode_text(pStruct->InvestorID);
            row["OpenRatioByMoney"] = pStruct->OpenRatioByMoney;
            row["OpenRatioByVolume"] = pStruct->OpenRatioByVolume;
            row["CloseRatioByMoney"] = pStruct->CloseRatioByMoney;
            row["CloseRatioByVolume"] = pStruct->CloseRatioByVolume;
            row["CloseTodayRatioByMoney"] = pStruct->CloseTodayRatioByMoney;
            row["CloseTodayRatioByVolume"] = pStruct->CloseTodayRatioByVolume;
            row["ExchangeID"] = decode_text(pStruct->ExchangeID);
            row["InvestUnitID"] = decode_text(pStruct->InvestUnitID);
            row["empty"] = false;
        } else {
            row["empty"] = true;
        }
        row["bIsLast"] = bIsLast;
        std::lock_guard<std::mutex> lk(mu_);
        auto it = pending_.find(nRequestID);
        if (it != pending_.end()) {
            it->second.rows.push_back(row);
            if (bIsLast) {
                it->second.done = true;
                request_name_by_id_.erase(nRequestID);
                cv_.notify_all();
            }
        }
    }
    void on_rsp_instrument_margin_rate(CThostFtdcInstrumentMarginRateField* pStruct, CThostFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast) {
        py::gil_scoped_acquire gil;
        py::dict row;
        if (pRspInfo && pRspInfo->ErrorID != 0) {
            row["ErrorID"] = pRspInfo->ErrorID;
            row["empty"] = true;
        } else if (pStruct) {
            row["InstrumentID"] = decode_text(pStruct->InstrumentID);
            row["BrokerID"] = decode_text(pStruct->BrokerID);
            row["InvestorID"] = decode_text(pStruct->InvestorID);
            row["HedgeFlag"] = pStruct->HedgeFlag;
            row["LongMarginRatioByMoney"] = pStruct->LongMarginRatioByMoney;
            row["LongMarginRatioByVolume"] = pStruct->LongMarginRatioByVolume;
            row["ShortMarginRatioByMoney"] = pStruct->ShortMarginRatioByMoney;
            row["ShortMarginRatioByVolume"] = pStruct->ShortMarginRatioByVolume;
            row["IsRelative"] = pStruct->IsRelative;
            row["ExchangeID"] = decode_text(pStruct->ExchangeID);
            row["InvestUnitID"] = decode_text(pStruct->InvestUnitID);
            row["empty"] = false;
        } else {
            row["empty"] = true;
        }
        row["bIsLast"] = bIsLast;
        std::lock_guard<std::mutex> lk(mu_);
        auto it = pending_.find(nRequestID);
        if (it != pending_.end()) {
            it->second.rows.push_back(row);
            if (bIsLast) {
                it->second.done = true;
                request_name_by_id_.erase(nRequestID);
                cv_.notify_all();
            }
        }
    }
    void on_rsp_qry_order(CThostFtdcOrderField* pStruct, CThostFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast) {
        py::gil_scoped_acquire gil;
        py::dict row;
        if (pRspInfo && pRspInfo->ErrorID != 0) {
            row["ErrorID"] = pRspInfo->ErrorID;
            row["empty"] = true;
        } else if (pStruct) {
            row["InstrumentID"] = decode_text(pStruct->InstrumentID);
            row["OrderRef"] = decode_text(pStruct->OrderRef);
            row["Direction"] = pStruct->Direction;
            row["CombOffsetFlag"] = pStruct->CombOffsetFlag;
            row["LimitPrice"] = pStruct->LimitPrice;
            row["VolumeTotalOriginal"] = pStruct->VolumeTotalOriginal;
            row["OrderSubmitStatus"] = pStruct->OrderSubmitStatus;
            row["OrderStatus"] = pStruct->OrderStatus;
            row["VolumeTraded"] = pStruct->VolumeTraded;
            row["VolumeTotal"] = pStruct->VolumeTotal;
            row["InsertDate"] = decode_text(pStruct->InsertDate);
            row["InsertTime"] = decode_text(pStruct->InsertTime);
            row["UpdateTime"] = decode_text(pStruct->UpdateTime);
            row["CancelTime"] = decode_text(pStruct->CancelTime);
            row["ExchangeID"] = decode_text(pStruct->ExchangeID);
            row["OrderSysID"] = decode_text(pStruct->OrderSysID);
            row["FrontID"] = pStruct->FrontID;
            row["SessionID"] = pStruct->SessionID;
            row["StatusMsg"] = decode_text(pStruct->StatusMsg);
            row["empty"] = false;
        } else {
            row["empty"] = true;
        }
        row["bIsLast"] = bIsLast;
        std::lock_guard<std::mutex> lk(mu_);
        auto it = pending_.find(nRequestID);
        if (it != pending_.end()) {
            it->second.rows.push_back(row);
            if (bIsLast) {
                it->second.done = true;
                request_name_by_id_.erase(nRequestID);
                cv_.notify_all();
            }
        }
    }
    void on_rsp_qry_trade(CThostFtdcTradeField* pStruct, CThostFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast) {
        py::gil_scoped_acquire gil;
        py::dict row;
        if (pRspInfo && pRspInfo->ErrorID != 0) {
            row["ErrorID"] = pRspInfo->ErrorID;
            row["empty"] = true;
        } else if (pStruct) {
            row["BrokerID"] = decode_text(pStruct->BrokerID);
            row["InvestorID"] = decode_text(pStruct->InvestorID);
            row["InstrumentID"] = decode_text(pStruct->InstrumentID);
            row["OrderRef"] = decode_text(pStruct->OrderRef);
            row["UserID"] = decode_text(pStruct->UserID);
            row["ExchangeID"] = decode_text(pStruct->ExchangeID);
            row["TradeID"] = decode_text(pStruct->TradeID);
            row["Direction"] = pStruct->Direction;
            row["OrderSysID"] = decode_text(pStruct->OrderSysID);
            row["OffsetFlag"] = pStruct->OffsetFlag;
            row["HedgeFlag"] = pStruct->HedgeFlag;
            row["Price"] = pStruct->Price;
            row["Volume"] = pStruct->Volume;
            row["TradeDate"] = decode_text(pStruct->TradeDate);
            row["TradeTime"] = decode_text(pStruct->TradeTime);
            row["TradeType"] = pStruct->TradeType;
            row["TradingDay"] = decode_text(pStruct->TradingDay);
            row["empty"] = false;
        } else {
            row["empty"] = true;
        }
        row["bIsLast"] = bIsLast;
        std::lock_guard<std::mutex> lk(mu_);
        auto it = pending_.find(nRequestID);
        if (it != pending_.end()) {
            it->second.rows.push_back(row);
            if (bIsLast) {
                it->second.done = true;
                request_name_by_id_.erase(nRequestID);
                cv_.notify_all();
            }
        }
    }
    void on_rsp_order_insert(CThostFtdcInputOrderField* pStruct, CThostFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast) {
        py::gil_scoped_acquire gil;
        py::dict row;
        if (pRspInfo && pRspInfo->ErrorID != 0) {
            row["ErrorID"] = pRspInfo->ErrorID;
        } else {
            row["ErrorID"] = 0;
        }
        if (pStruct) {
            row["InstrumentID"] = decode_text(pStruct->InstrumentID);
            row["OrderRef"] = decode_text(pStruct->OrderRef);
            row["Direction"] = pStruct->Direction;
            row["CombOffsetFlag"] = pStruct->CombOffsetFlag;
            row["LimitPrice"] = pStruct->LimitPrice;
            row["VolumeTotalOriginal"] = pStruct->VolumeTotalOriginal;
        }
        row["RequestID"] = nRequestID;
        row["bIsLast"] = bIsLast;
        if (bIsLast) {
            std::lock_guard<std::mutex> lk(mu_);
            request_name_by_id_.erase(nRequestID);
        }
        emit_event("OnRspOrderInsert", row);
    }
    void on_rsp_order_action(CThostFtdcInputOrderActionField* pStruct, CThostFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast) {
        py::gil_scoped_acquire gil;
        py::dict row;
        if (pRspInfo && pRspInfo->ErrorID != 0) {
            row["ErrorID"] = pRspInfo->ErrorID;
        } else {
            row["ErrorID"] = 0;
        }
        if (pStruct) {
            row["ExchangeID"] = decode_text(pStruct->ExchangeID);
            row["InstrumentID"] = decode_text(pStruct->InstrumentID);
            row["OrderSysID"] = decode_text(pStruct->OrderSysID);
            row["OrderRef"] = decode_text(pStruct->OrderRef);
        }
        row["RequestID"] = nRequestID;
        row["bIsLast"] = bIsLast;
        {
            std::lock_guard<std::mutex> lk(mu_);
            auto it = pending_.find(nRequestID);
            if (it != pending_.end()) {
                it->second.rows.push_back(row);
                if (bIsLast) {
                    it->second.done = true;
                    request_name_by_id_.erase(nRequestID);
                    cv_.notify_all();
                }
            }
        }
        emit_event("OnRspOrderAction", row);
    }
    void on_rtn_order(CThostFtdcOrderField* pStruct) {
        if (!pStruct) {
            return;
        }
        py::gil_scoped_acquire gil;
        py::dict row;
        row["InstrumentID"] = decode_text(pStruct->InstrumentID);
        row["OrderRef"] = decode_text(pStruct->OrderRef);
        row["Direction"] = pStruct->Direction;
        row["CombOffsetFlag"] = pStruct->CombOffsetFlag;
        row["LimitPrice"] = pStruct->LimitPrice;
        row["VolumeTotalOriginal"] = pStruct->VolumeTotalOriginal;
        row["OrderSubmitStatus"] = pStruct->OrderSubmitStatus;
        row["OrderStatus"] = pStruct->OrderStatus;
        row["VolumeTraded"] = pStruct->VolumeTraded;
        row["VolumeTotal"] = pStruct->VolumeTotal;
        row["InsertDate"] = decode_text(pStruct->InsertDate);
        row["InsertTime"] = decode_text(pStruct->InsertTime);
        row["ExchangeID"] = decode_text(pStruct->ExchangeID);
        row["OrderSysID"] = decode_text(pStruct->OrderSysID);
        row["FrontID"] = pStruct->FrontID;
        row["SessionID"] = pStruct->SessionID;
        row["UserID"] = decode_text(pStruct->UserID);
        row["StatusMsg"] = decode_text(pStruct->StatusMsg);
        emit_event("OnRtnOrder", row);
    }
    void on_rtn_trade(CThostFtdcTradeField* pStruct) {
        if (!pStruct) {
            return;
        }
        py::gil_scoped_acquire gil;
        py::dict row;
        row["BrokerID"] = decode_text(pStruct->BrokerID);
        row["InvestorID"] = decode_text(pStruct->InvestorID);
        row["InstrumentID"] = decode_text(pStruct->InstrumentID);
        row["OrderRef"] = decode_text(pStruct->OrderRef);
        row["UserID"] = decode_text(pStruct->UserID);
        row["ExchangeID"] = decode_text(pStruct->ExchangeID);
        row["TradeID"] = decode_text(pStruct->TradeID);
        row["Direction"] = pStruct->Direction;
        row["OrderSysID"] = decode_text(pStruct->OrderSysID);
        row["OffsetFlag"] = pStruct->OffsetFlag;
        row["HedgeFlag"] = pStruct->HedgeFlag;
        row["Price"] = pStruct->Price;
        row["Volume"] = pStruct->Volume;
        row["TradeDate"] = decode_text(pStruct->TradeDate);
        row["TradeTime"] = decode_text(pStruct->TradeTime);
        row["TradeType"] = pStruct->TradeType;
        row["TradingDay"] = decode_text(pStruct->TradingDay);
        emit_event("OnRtnTrade", row);
    }
    void on_rsp_sub_market_data(CThostFtdcSpecificInstrumentField* pSpecificInstrument, CThostFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast, bool un_sub) {
        py::gil_scoped_acquire gil;
        py::dict row;
        if (pRspInfo && pRspInfo->ErrorID != 0) {
            row["ErrorID"] = pRspInfo->ErrorID;
            row["empty"] = true;
        } else if (pSpecificInstrument) {
            row["InstrumentID"] = decode_text(pSpecificInstrument->InstrumentID);
            row["empty"] = false;
        } else {
            row["empty"] = true;
        }
        row["bIsLast"] = bIsLast;
        std::lock_guard<std::mutex> lk(mu_);
        auto it = pending_.find(nRequestID);
        if (it != pending_.end()) {
            it->second.rows.push_back(row);
            if (bIsLast) {
                it->second.done = true;
                request_name_by_id_.erase(nRequestID);
                cv_.notify_all();
            }
        }
        (void)un_sub;
    }
    void on_rtn_depth_market_data(CThostFtdcDepthMarketDataField* pData) {
        if (!pData) {
            return;
        }
        py::gil_scoped_acquire gil;
        py::dict row;
        row["TradingDay"] = decode_text(pData->TradingDay);
        row["InstrumentID"] = decode_text(pData->InstrumentID);
        row["LastPrice"] = pData->LastPrice;
        row["OpenPrice"] = pData->OpenPrice;
        row["HighestPrice"] = pData->HighestPrice;
        row["LowestPrice"] = pData->LowestPrice;
        row["Volume"] = pData->Volume;
        row["Turnover"] = pData->Turnover;
        row["OpenInterest"] = pData->OpenInterest;
        row["UpperLimitPrice"] = pData->UpperLimitPrice;
        row["LowerLimitPrice"] = pData->LowerLimitPrice;
        row["BidPrice1"] = pData->BidPrice1;
        row["BidVolume1"] = pData->BidVolume1;
        row["AskPrice1"] = pData->AskPrice1;
        row["AskVolume1"] = pData->AskVolume1;
        row["ActionDay"] = decode_text(pData->ActionDay);
        row["UpdateTime"] = decode_text(pData->ActionDay).cast<std::string>() + " " + decode_text(pData->UpdateTime).cast<std::string>() + ":" + std::to_string(pData->UpdateMillisec * 1000);
        emit_event("OnRtnDepthMarketData", row);
    }

  private:
    struct Pending {
        std::vector<py::dict> rows;
        bool done{false};
    };
    void enforce_query_throttle() {
        std::unique_lock<std::mutex> lk(mu_);
        auto now = std::chrono::steady_clock::now();
        auto next_allowed = last_query_at_ + std::chrono::milliseconds(1000);
        if (now < next_allowed) {
            auto wait_dur = next_allowed - now;
            lk.unlock();
            std::this_thread::sleep_for(wait_dur);
            lk.lock();
        }
        last_query_at_ = std::chrono::steady_clock::now();
    }
    void wait_login(bool trade, bool market) {
        std::unique_lock<std::mutex> lk(mu_);
        const auto timeout_ms = std::chrono::milliseconds(request_timeout_ms_);
        bool ok = cv_login_.wait_for(lk, timeout_ms, [&] {
            bool trade_ok = !trade || trade_login_;
            bool market_ok = !market || market_login_;
            return trade_ok && market_ok;
        });
        if (!ok) {
            throw std::runtime_error("CTP login timeout");
        }
    }
    void send_trade_login(int req_id) {
        CThostFtdcReqUserLoginField req{};
        copy_cstr(req.BrokerID, sizeof(req.BrokerID), broker_id_);
        copy_cstr(req.UserID, sizeof(req.UserID), investor_id_);
        copy_cstr(req.Password, sizeof(req.Password), password_);
        trader_api_->ReqUserLogin(&req, req_id);
    }
    int send_settlement_info_confirm(int req_id) {
        CThostFtdcSettlementInfoConfirmField req{};
        copy_cstr(req.BrokerID, sizeof(req.BrokerID), broker_id_);
        copy_cstr(req.InvestorID, sizeof(req.InvestorID), investor_id_);
        return trader_api_->ReqSettlementInfoConfirm(&req, req_id);
    }
    int dispatch_request(const std::string& req_name, int request_id, const py::object& payload) {
        py::dict payload_dict;
        if (py::isinstance<py::dict>(payload)) {
            payload_dict = payload.cast<py::dict>();
        }
        if (req_name == "ReqQryTradingAccount") {
            CThostFtdcQryTradingAccountField req{};
            copy_cstr(req.BrokerID, sizeof(req.BrokerID), broker_id_);
            copy_cstr(req.InvestorID, sizeof(req.InvestorID), investor_id_);
            return trader_api_->ReqQryTradingAccount(&req, request_id);
        }
        if (req_name == "ReqQryInvestorPositionDetail") {
            CThostFtdcQryInvestorPositionDetailField req{};
            copy_cstr(req.BrokerID, sizeof(req.BrokerID), broker_id_);
            copy_cstr(req.InvestorID, sizeof(req.InvestorID), investor_id_);
            copy_cstr(req.InstrumentID, sizeof(req.InstrumentID), get_str(payload_dict, "InstrumentID"));
            return trader_api_->ReqQryInvestorPositionDetail(&req, request_id);
        }
        if (req_name == "ReqQryInstrument") {
            CThostFtdcQryInstrumentField req{};
            copy_cstr(req.InstrumentID, sizeof(req.InstrumentID), get_str(payload_dict, "InstrumentID"));
            copy_cstr(req.ExchangeID, sizeof(req.ExchangeID), get_str(payload_dict, "ExchangeID"));
            return trader_api_->ReqQryInstrument(&req, request_id);
        }
        if (req_name == "ReqQryInstrumentCommissionRate") {
            CThostFtdcQryInstrumentCommissionRateField req{};
            copy_cstr(req.BrokerID, sizeof(req.BrokerID), broker_id_);
            copy_cstr(req.InvestorID, sizeof(req.InvestorID), investor_id_);
            copy_cstr(req.InstrumentID, sizeof(req.InstrumentID), get_str(payload_dict, "InstrumentID"));
            copy_cstr(req.ExchangeID, sizeof(req.ExchangeID), get_str(payload_dict, "ExchangeID"));
            copy_cstr(req.InvestUnitID, sizeof(req.InvestUnitID), get_str(payload_dict, "InvestUnitID"));
            return trader_api_->ReqQryInstrumentCommissionRate(&req, request_id);
        }
        if (req_name == "ReqQryInstrumentMarginRate") {
            CThostFtdcQryInstrumentMarginRateField req{};
            copy_cstr(req.BrokerID, sizeof(req.BrokerID), broker_id_);
            copy_cstr(req.InvestorID, sizeof(req.InvestorID), investor_id_);
            copy_cstr(req.InstrumentID, sizeof(req.InstrumentID), get_str(payload_dict, "InstrumentID"));
            auto hedge_flag = get_str(payload_dict, "HedgeFlag");
            req.HedgeFlag = hedge_flag.empty() ? THOST_FTDC_HF_Speculation : hedge_flag[0];
            copy_cstr(req.ExchangeID, sizeof(req.ExchangeID), get_str(payload_dict, "ExchangeID"));
            copy_cstr(req.InvestUnitID, sizeof(req.InvestUnitID), get_str(payload_dict, "InvestUnitID"));
            return trader_api_->ReqQryInstrumentMarginRate(&req, request_id);
        }
        if (req_name == "ReqQryOrder") {
            CThostFtdcQryOrderField req{};
            copy_cstr(req.BrokerID, sizeof(req.BrokerID), broker_id_);
            copy_cstr(req.InvestorID, sizeof(req.InvestorID), investor_id_);
            copy_cstr(req.InstrumentID, sizeof(req.InstrumentID), get_str(payload_dict, "InstrumentID"));
            copy_cstr(req.ExchangeID, sizeof(req.ExchangeID), get_str(payload_dict, "ExchangeID"));
            copy_cstr(req.OrderSysID, sizeof(req.OrderSysID), get_str(payload_dict, "OrderSysID"));
            copy_cstr(req.InsertTimeStart, sizeof(req.InsertTimeStart), get_str(payload_dict, "InsertTimeStart"));
            copy_cstr(req.InsertTimeEnd, sizeof(req.InsertTimeEnd), get_str(payload_dict, "InsertTimeEnd"));
            copy_cstr(req.InvestUnitID, sizeof(req.InvestUnitID), get_str(payload_dict, "InvestUnitID"));
            return trader_api_->ReqQryOrder(&req, request_id);
        }
        if (req_name == "ReqQryTrade") {
            CThostFtdcQryTradeField req{};
            copy_cstr(req.BrokerID, sizeof(req.BrokerID), broker_id_);
            copy_cstr(req.InvestorID, sizeof(req.InvestorID), investor_id_);
            copy_cstr(req.InstrumentID, sizeof(req.InstrumentID), get_str(payload_dict, "InstrumentID"));
            copy_cstr(req.ExchangeID, sizeof(req.ExchangeID), get_str(payload_dict, "ExchangeID"));
            copy_cstr(req.TradeID, sizeof(req.TradeID), get_str(payload_dict, "TradeID"));
            copy_cstr(req.TradeTimeStart, sizeof(req.TradeTimeStart), get_str(payload_dict, "TradeTimeStart"));
            copy_cstr(req.TradeTimeEnd, sizeof(req.TradeTimeEnd), get_str(payload_dict, "TradeTimeEnd"));
            copy_cstr(req.InvestUnitID, sizeof(req.InvestUnitID), get_str(payload_dict, "InvestUnitID"));
            return trader_api_->ReqQryTrade(&req, request_id);
        }
        if (req_name == "SubscribeMarketData") {
            std::vector<std::string> ids;
            if (py::isinstance<py::list>(payload)) {
                for (auto item : payload.cast<py::list>()) {
                    ids.push_back(py::str(item).cast<std::string>());
                }
            }
            std::vector<char*> ptrs;
            ptrs.reserve(ids.size());
            for (auto& s : ids) {
                ptrs.push_back(s.data());
            }
            return md_api_->SubscribeMarketData(ptrs.data(), static_cast<int>(ptrs.size()));
        }
        if (req_name == "UnSubscribeMarketData") {
            std::vector<std::string> ids;
            if (py::isinstance<py::list>(payload)) {
                for (auto item : payload.cast<py::list>()) {
                    ids.push_back(py::str(item).cast<std::string>());
                }
            }
            std::vector<char*> ptrs;
            ptrs.reserve(ids.size());
            for (auto& s : ids) {
                ptrs.push_back(s.data());
            }
            return md_api_->UnSubscribeMarketData(ptrs.data(), static_cast<int>(ptrs.size()));
        }
        if (req_name == "ReqOrderInsert") {
            CThostFtdcInputOrderField req{};
            copy_cstr(req.InstrumentID, sizeof(req.InstrumentID), get_str(payload_dict, "InstrumentID"));
            copy_cstr(req.OrderRef, sizeof(req.OrderRef), get_str(payload_dict, "OrderRef"));
            req.Direction = get_str(payload_dict, "Direction").empty() ? '\0' : get_str(payload_dict, "Direction")[0];
            req.LimitPrice = get_double(payload_dict, "LimitPrice", 0.0);
            req.VolumeTotalOriginal = get_int(payload_dict, "VolumeTotalOriginal", 0);
            req.OrderPriceType = THOST_FTDC_OPT_LimitPrice;
            copy_cstr(req.BrokerID, sizeof(req.BrokerID), broker_id_);
            copy_cstr(req.InvestorID, sizeof(req.InvestorID), investor_id_);
            auto cof = get_str(payload_dict, "CombOffsetFlag");
            req.CombOffsetFlag[0] = cof.empty() ? '\0' : cof[0];
            req.CombHedgeFlag[0] = THOST_FTDC_HF_Speculation;
            req.VolumeCondition = THOST_FTDC_VC_AV;
            req.MinVolume = 1;
            req.ForceCloseReason = THOST_FTDC_FCC_NotForceClose;
            req.ContingentCondition = THOST_FTDC_CC_Immediately;
            req.IsAutoSuspend = 1;
            req.UserForceClose = 0;
            req.TimeCondition = THOST_FTDC_TC_GFD;
            copy_cstr(req.IPAddress, sizeof(req.IPAddress), ip_);
            copy_cstr(req.MacAddress, sizeof(req.MacAddress), mac_);
            return trader_api_->ReqOrderInsert(&req, request_id);
        }
        if (req_name == "ReqOrderAction") {
            CThostFtdcInputOrderActionField req{};
            copy_cstr(req.BrokerID, sizeof(req.BrokerID), broker_id_);
            copy_cstr(req.InvestorID, sizeof(req.InvestorID), investor_id_);
            copy_cstr(req.ExchangeID, sizeof(req.ExchangeID), get_str(payload_dict, "ExchangeID"));
            copy_cstr(req.UserID, sizeof(req.UserID), get_str(payload_dict, "UserID", investor_id_));
            copy_cstr(req.InstrumentID, sizeof(req.InstrumentID), get_str(payload_dict, "InstrumentID"));
            copy_cstr(req.OrderSysID, sizeof(req.OrderSysID), get_str(payload_dict, "OrderSysID"));
            req.OrderActionRef = request_id + 1;
            req.ActionFlag = THOST_FTDC_AF_Delete;
            copy_cstr(req.IPAddress, sizeof(req.IPAddress), ip_);
            copy_cstr(req.MacAddress, sizeof(req.MacAddress), mac_);
            return trader_api_->ReqOrderAction(&req, request_id);
        }
        return -999;
    }
    void emit_event(const std::string& topic, const py::dict& row) {
        if (event_callback_.is_none()) {
            return;
        }
        try {
            event_callback_(topic, row);
        } catch (const py::error_already_set& e) {
            PyErr_WriteUnraisable(e.value().ptr());
        }
    }
    friend class TraderSpi;
    friend class MdSpi;
    std::mutex mu_;
    std::condition_variable cv_;
    std::condition_variable cv_login_;
    std::unordered_map<int, Pending> pending_;
    std::unordered_map<int, std::string> request_name_by_id_;
    CThostFtdcTraderApi* trader_api_{nullptr};
    CThostFtdcMdApi* md_api_{nullptr};
    TraderSpi* trader_spi_{nullptr};
    MdSpi* md_spi_{nullptr};
    bool started_{false};
    bool trade_login_{false};
    bool market_login_{false};
    int request_timeout_ms_{10000};
    py::object event_callback_{py::none()};
    std::string trade_front_;
    std::string market_front_;
    std::string broker_id_;
    std::string investor_id_;
    std::string password_;
    std::string appid_;
    std::string authcode_;
    std::string userinfo_;
    std::string ip_;
    std::string mac_;
    std::string flow_path_{"d:/Github/trader/native/ctp_bridge/flow"};
    std::chrono::steady_clock::time_point last_query_at_{};
};
// ----- TraderSpi impl -----
void TraderSpi::OnFrontConnected() { owner_->on_trade_front_connected(); }
void TraderSpi::OnFrontDisconnected(int nReason) { owner_->on_trade_front_disconnected(nReason); }
void TraderSpi::OnRspAuthenticate(CThostFtdcRspAuthenticateField* /*pRspAuthenticateField*/, CThostFtdcRspInfoField* pRspInfo, int nRequestID, bool /*bIsLast*/) { owner_->on_trade_auth_rsp(pRspInfo, nRequestID); }
void TraderSpi::OnRspUserLogin(CThostFtdcRspUserLoginField* pRspUserLogin, CThostFtdcRspInfoField* pRspInfo, int nRequestID, bool /*bIsLast*/) { owner_->on_trade_login_rsp(pRspUserLogin, pRspInfo, nRequestID); }
void TraderSpi::OnRspError(CThostFtdcRspInfoField* pRspInfo, int nRequestID, bool /*bIsLast*/) {
    if (pRspInfo) {
        owner_->on_rsp_error(nRequestID, pRspInfo->ErrorID, pRspInfo->ErrorMsg);
    }
}
void TraderSpi::OnRspQryTradingAccount(CThostFtdcTradingAccountField* pStruct, CThostFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast) { owner_->on_rsp_trading_account(pStruct, pRspInfo, nRequestID, bIsLast); }
void TraderSpi::OnRspQryInvestorPositionDetail(CThostFtdcInvestorPositionDetailField* pStruct, CThostFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast) { owner_->on_rsp_investor_position_detail(pStruct, pRspInfo, nRequestID, bIsLast); }
void TraderSpi::OnRspQryInstrument(CThostFtdcInstrumentField* pStruct, CThostFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast) { owner_->on_rsp_instrument(pStruct, pRspInfo, nRequestID, bIsLast); }
void TraderSpi::OnRspQryInstrumentCommissionRate(CThostFtdcInstrumentCommissionRateField* pStruct, CThostFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast) { owner_->on_rsp_instrument_commission_rate(pStruct, pRspInfo, nRequestID, bIsLast); }
void TraderSpi::OnRspQryInstrumentMarginRate(CThostFtdcInstrumentMarginRateField* pStruct, CThostFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast) { owner_->on_rsp_instrument_margin_rate(pStruct, pRspInfo, nRequestID, bIsLast); }
void TraderSpi::OnRspQryOrder(CThostFtdcOrderField* pStruct, CThostFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast) { owner_->on_rsp_qry_order(pStruct, pRspInfo, nRequestID, bIsLast); }
void TraderSpi::OnRspQryTrade(CThostFtdcTradeField* pStruct, CThostFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast) { owner_->on_rsp_qry_trade(pStruct, pRspInfo, nRequestID, bIsLast); }
void TraderSpi::OnRspSettlementInfoConfirm(CThostFtdcSettlementInfoConfirmField* pSettlementInfoConfirm, CThostFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast) { owner_->on_rsp_settlement_info_confirm(pSettlementInfoConfirm, pRspInfo, nRequestID, bIsLast); }
void TraderSpi::OnRspOrderInsert(CThostFtdcInputOrderField* pStruct, CThostFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast) { owner_->on_rsp_order_insert(pStruct, pRspInfo, nRequestID, bIsLast); }
void TraderSpi::OnRspOrderAction(CThostFtdcInputOrderActionField* pStruct, CThostFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast) { owner_->on_rsp_order_action(pStruct, pRspInfo, nRequestID, bIsLast); }
void TraderSpi::OnRtnOrder(CThostFtdcOrderField* pStruct) { owner_->on_rtn_order(pStruct); }
void TraderSpi::OnRtnTrade(CThostFtdcTradeField* pStruct) { owner_->on_rtn_trade(pStruct); }
// ----- MdSpi impl -----
void MdSpi::OnFrontConnected() { owner_->on_market_front_connected(); }
void MdSpi::OnFrontDisconnected(int nReason) { owner_->on_market_front_disconnected(nReason); }
void MdSpi::OnRspUserLogin(CThostFtdcRspUserLoginField* /*pRspUserLogin*/, CThostFtdcRspInfoField* pRspInfo, int /*nRequestID*/, bool /*bIsLast*/) { owner_->on_market_login_rsp(pRspInfo); }
void MdSpi::OnRspError(CThostFtdcRspInfoField* pRspInfo, int nRequestID, bool /*bIsLast*/) {
    if (pRspInfo) {
        owner_->on_rsp_error(nRequestID, pRspInfo->ErrorID, pRspInfo->ErrorMsg);
    }
}
void MdSpi::OnRspSubMarketData(CThostFtdcSpecificInstrumentField* pSpecificInstrument, CThostFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast) { owner_->on_rsp_sub_market_data(pSpecificInstrument, pRspInfo, nRequestID, bIsLast, false); }
void MdSpi::OnRspUnSubMarketData(CThostFtdcSpecificInstrumentField* pSpecificInstrument, CThostFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast) { owner_->on_rsp_sub_market_data(pSpecificInstrument, pRspInfo, nRequestID, bIsLast, true); }
void MdSpi::OnRtnDepthMarketData(CThostFtdcDepthMarketDataField* pData) { owner_->on_rtn_depth_market_data(pData); }
PYBIND11_MODULE(ctp_bridge_native, m) {
    m.doc() = "CTP native bridge (pybind11)";
    py::class_<CtpClient>(m, "CtpClient").def(py::init<>()).def("configure", &CtpClient::configure).def("start", &CtpClient::start).def("stop", &CtpClient::stop).def("set_event_callback", &CtpClient::set_event_callback).def("request", &CtpClient::request);
}
