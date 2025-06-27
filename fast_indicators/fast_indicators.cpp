#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <vector>
#include <cmath>

namespace py = pybind11;

py::array_t<double> rsi(py::array_t<double, py::array::c_style | py::array::forcecast> close, int periodo){
    auto buf = close.request();
    const size_t n = buf.shape[0];
    const double* c = static_cast<double*>(buf.ptr);
    if(n < static_cast<size_t>(periodo) + 1){
        return py::array_t<double>(0);
    }
    std::vector<double> r(n, std::numeric_limits<double>::quiet_NaN());
    double alpha = 1.0 / periodo;
    double prev_gain = std::numeric_limits<double>::quiet_NaN();
    double prev_loss = std::numeric_limits<double>::quiet_NaN();
    for(size_t i=1;i<n;++i){
        double change = c[i] - c[i-1];
        double gain = change > 0 ? change : 0.0;
        double loss = change < 0 ? -change : 0.0;
        if(std::isnan(prev_gain)){
            prev_gain = gain;
            prev_loss = loss;
        } else {
            prev_gain = (1.0 - alpha) * prev_gain + alpha * gain;
            prev_loss = (1.0 - alpha) * prev_loss + alpha * loss;
        }
        if(i >= static_cast<size_t>(periodo)){
            double rs = prev_gain / prev_loss;
            r[i] = 100.0 - 100.0 / (1.0 + rs);
        }
    }
    auto result = py::array_t<double>(n);
    std::memcpy(result.mutable_data(), r.data(), n * sizeof(double));
    return result;
}

double atr(py::array_t<double, py::array::c_style | py::array::forcecast> high,
           py::array_t<double, py::array::c_style | py::array::forcecast> low,
           py::array_t<double, py::array::c_style | py::array::forcecast> close,
           int periodo){
    auto h = high.unchecked<1>();
    auto l = low.unchecked<1>();
    auto c = close.unchecked<1>();
    const size_t n = h.shape(0);
    if(n < static_cast<size_t>(periodo) + 1){
        return std::numeric_limits<double>::quiet_NaN();
    }
    std::vector<double> tr(n);
    tr[0] = h(0) - l(0);
    for(size_t i=1;i<n;++i){
        double tr1 = h(i) - l(i);
        double tr2 = std::fabs(h(i) - c(i-1));
        double tr3 = std::fabs(l(i) - c(i-1));
        tr[i] = std::max({tr1, tr2, tr3});
    }
    double alpha = 1.0 / periodo;
    double ema = tr[0];
    for(size_t i=1;i<n;++i){
        ema = (1.0 - alpha) * ema + alpha * tr[i];
    }
    return ema;
}

double slope(py::array_t<double, py::array::c_style | py::array::forcecast> close,
             int periodo){
    auto c = close.unchecked<1>();
    const size_t n = c.shape(0);
    if(n < static_cast<size_t>(periodo) || periodo < 2){
        return 0.0;
    }
    size_t offset = n - periodo;
    double sum_x = (periodo - 1) * periodo / 2.0;
    double sum_x2 = (periodo - 1) * periodo * (2 * periodo - 1) / 6.0;
    double sum_y = 0.0;
    double sum_xy = 0.0;
    for(int i=0;i<periodo;i++){
        double y = c(offset + i);
        sum_y += y;
        sum_xy += i * y;
    }
    double denom = periodo * sum_x2 - sum_x * sum_x;
    double m = (periodo * sum_xy - sum_x * sum_y) / denom;
    return m;
}

PYBIND11_MODULE(fast_indicators_ext, m){
    m.def("rsi", &rsi, "Compute RSI", py::arg("close"), py::arg("periodo"));
    m.def("atr", &atr, "Compute ATR", py::arg("high"), py::arg("low"), py::arg("close"), py::arg("periodo"));
    m.def("slope", &slope, "Compute slope", py::arg("close"), py::arg("periodo"));
}