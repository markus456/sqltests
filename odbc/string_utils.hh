#pragma once
#include <string>
#include <sstream>
#include <iomanip>
#include <algorithm>
#include <cstring>
#include <cstdarg>

namespace mxb
{
    /**
     * Join objects into a string delimited by separators
     *
     * @param container Container that provides iterators, stored value must support writing to ostream with
     *                  operator<<
     * @param separator Value used as the separator
     * @param quotation Quotation marker used to quote the values
     *
     * @return String created by joining all values and delimiting them with `separator` (no trailing delimiter)
     */
    template <class T>
    std::string join(const T &container, const std::string &separator = ",", const std::string &quotation = "")
    {
        std::ostringstream ss;
        auto it = std::begin(container);

        if (it != std::end(container))
        {
            ss << quotation << *it++ << quotation;

            while (it != std::end(container))
            {
                ss << separator << quotation << *it++ << quotation;
            }
        }

        return ss.str();
    }

    /**
     * Tokenize a string
     *
     * @param str   String to tokenize
     * @param delim List of delimiters (see strtok(3))
     *
     * @return List of tokenized strings
     */
    inline std::vector<std::string> strtok(std::string str, const char *delim)
    {
        std::vector<std::string> rval;
        char *save_ptr;
        char *tok = strtok_r(&str[0], delim, &save_ptr);

        while (tok)
        {
            rval.emplace_back(tok);
            tok = strtok_r(NULL, delim, &save_ptr);
        }

        return rval;
    }

    /**
     * Transform and join objects into a string delimited by separators
     *
     * @param container Container that provides iterators, stored value must support writing to ostream with
     *                  operator<<
     * @param op        Unary operation to perform on all container values
     * @param separator Value used as the separator
     * @param quotation Quotation marker used to quote the values
     *
     * @return String created by joining all values and delimiting them with `separator` (no trailing delimiter)
     */
    template <class T, class UnaryOperator>
    std::string transform_join(const T &container, UnaryOperator op,
                               const std::string &separator = ",", const std::string &quotation = "")
    {
        std::ostringstream ss;
        auto it = std::begin(container);

        if (it != std::end(container))
        {
            ss << quotation << op(*it++) << quotation;

            while (it != std::end(container))
            {
                ss << separator << quotation << op(*it++) << quotation;
            }
        }

        return ss.str();
    }

    static inline std::string string_vprintf(const char *format, va_list args)
    {
        /* Use 'vsnprintf' for the formatted printing. It outputs the optimal buffer length - 1. */
        va_list args_copy;
        va_copy(args_copy, args);
        int characters = vsnprintf(nullptr, 0, format, args_copy);
        va_end(args_copy);

        std::string rval;
        if (characters > 0)
        {
            // 'characters' does not include the \0-byte.
            rval.resize(characters); // The final "length" of the string
            // Write directly to the string internal array, avoiding any temporary arrays.
            vsnprintf(&rval[0], characters + 1, format, args);
        }
        return rval;
    }

    static inline std::string string_printf(const char *format, ...)
    {
        va_list args;
        va_start(args, format);
        std::string rval = mxb::string_vprintf(format, args);
        va_end(args);
        return rval;
    }

}

template <char Delim = ' ', class T, class... Args>
std::string to_str(T &&t, Args... args)
{
    std::ostringstream ss;
    ss << t;
    ((ss << Delim << args), ...);
    return ss.str();
}

template <class... Args>
size_t get_len(Args... args)
{
    return std::max({to_str(args).size()...});
}

template <class... Args>
std::string pretty_print(size_t width, Args... args)
{
    std::ostringstream ss;
    ss << '|';
    ((ss << std::setw(width) << args << '|'), ...);
    return ss.str();
}

static inline std::string quote(std::string_view str, char ch = '"')
{
    std::ostringstream ss;
    ss << ch << str << ch;
    return ss.str();
}
