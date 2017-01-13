/*******************************************************************************
 * tlx/cmdline_parser.cpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2013-2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#include <tlx/cmdline_parser.hpp>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

#include <tlx/string/parse_si_iec_units.hpp>

namespace tlx {

/******************************************************************************/
// Argument and Struct Hierarchy below it.

//! base class of all options and parameters
struct CmdlineParser::Argument {

    //! single letter short option, or 0 is none
    char        key_;
    //! long option key or name for parameters
    std::string longkey_;
    //! option type description, e.g. "<#>" to indicate numbers
    std::string keytype_;
    //! longer description, which will be wrapped
    std::string desc_;
    //! required, process() fails if the option/parameter is not found.
    bool        required_;
    //! found during processing of command line
    bool        found_ = false;
    //! repeated argument, i.e. std::vector<std::string>
    bool        repeated_ = false;

    //! contructor filling most attributes
    Argument(char key, const std::string& longkey, const std::string& keytype,
             const std::string& desc, bool required)
        : key_(key), longkey_(longkey), keytype_(keytype), desc_(desc),
          required_(required) { }

    //! empty virtual destructor
    virtual ~Argument() = default;

    //! return formatted type name to user
    virtual const char * type_name() const = 0;

    //! process one item from command line for this argument
    virtual bool process(int& argc, const char* const*& argv) = 0; // NOLINT

    //! format value to ostream
    virtual void print_value(std::ostream& os) const = 0;

    //! return 'longkey [keytype]'
    std::string param_text() const {
        std::string s = longkey_;
        if (keytype_.size()) {
            s += ' ' + keytype_;
        }
        return s;
    }

    //! return '-s, --longkey [keytype]'
    std::string option_text() const {
        std::string s;
        if (key_) {
            s += '-', s += key_, s += ", ";
        }
        s += "--", s += longkey_;
        if (keytype_.size()) {
            s += ' ' + keytype_;
        }
        return s;
    }
};

//! specialization of argument for boolean flags (can only be set to true).
struct CmdlineParser::ArgumentBool final : public Argument {
    //! reference to boolean to set to true
    bool& dest_;

    //! contructor filling most attributes
    ArgumentBool(char key, const std::string& longkey,
                 const std::string& keytype, const std::string& desc,
                 bool required, bool& dest) // NOLINT
        : Argument(key, longkey, keytype, desc, required), dest_(dest) { }

    const char * type_name() const final { return "bool"; }

    //! "process" argument: just set to true, no argument is used.
    bool process(int&, const char* const*&) final {
        dest_ = true;
        return true;
    }

    void print_value(std::ostream& os) const final {
        os << (dest_ ? "true" : "false");
    }
};

//! specialization of argument for integer options or parameters
struct CmdlineParser::ArgumentInt final : public Argument {
    int& dest_;

    //! contructor filling most attributes
    ArgumentInt(char key, const std::string& longkey,
                const std::string& keytype, const std::string& desc,
                bool required, int& dest) // NOLINT
        : Argument(key, longkey, keytype, desc, required), dest_(dest) { }

    const char * type_name() const final { return "integer"; }

    //! parse signed integer using sscanf.
    bool process(int& argc, const char* const*& argv) final { // NOLINT
        if (argc == 0)
            return false;
        if (sscanf(argv[0], "%d", &dest_) == 1) {
            --argc, ++argv;
            return true;
        }
        else {
            return false;
        }
    }

    void print_value(std::ostream& os) const final { os << dest_; }
};

//! specialization of argument for unsigned integer options or parameters
struct CmdlineParser::ArgumentUnsigned final : public Argument {
    unsigned int& dest_;

    //! contructor filling most attributes
    ArgumentUnsigned(char key, const std::string& longkey,
                     const std::string& keytype, const std::string& desc,
                     bool required, unsigned int& dest) // NOLINT
        : Argument(key, longkey, keytype, desc, required), dest_(dest) { }

    const char * type_name() const final { return "unsigned"; }

    //! parse unsigned integer using sscanf.
    bool process(int& argc, const char* const*& argv) final { // NOLINT
        if (argc == 0)
            return false;
        if (sscanf(argv[0], "%u", &dest_) == 1) {
            --argc, ++argv;
            return true;
        }
        else {
            return false;
        }
    }

    void print_value(std::ostream& os) const final { os << dest_; }
};

//! specialization of argument for size_t options or parameters
struct CmdlineParser::ArgumentSizeT final : public Argument {
    size_t& dest_;

    //! contructor filling most attributes
    ArgumentSizeT(char key, const std::string& longkey,
                  const std::string& keytype, const std::string& desc,
                  bool required, size_t& dest) // NOLINT
        : Argument(key, longkey, keytype, desc, required), dest_(dest) { }

    const char * type_name() const final { return "size_t"; }

    //! parse size_t using sscanf.
    bool process(int& argc, const char* const*& argv) final { // NOLINT
        if (argc == 0)
            return false;
        if (sscanf(argv[0], "%zu", &dest_) == 1) {
            --argc, ++argv;
            return true;
        }
        else {
            return false;
        }
    }

    void print_value(std::ostream& os) const final { os << dest_; }
};

//! specialization of argument for float options or parameters
struct CmdlineParser::ArgumentFloat final : public Argument {
    float& dest_;

    //! contructor filling most attributes
    ArgumentFloat(char key, const std::string& longkey,
                  const std::string& keytype, const std::string& desc,
                  bool required, float& dest) // NOLINT
        : Argument(key, longkey, keytype, desc, required), dest_(dest) { }

    const char * type_name() const final { return "float"; }

    //! parse unsigned integer using sscanf.
    bool process(int& argc, const char* const*& argv) final { // NOLINT
        if (argc == 0)
            return false;
        if (sscanf(argv[0], "%f", &dest_) == 1) {
            --argc, ++argv;
            return true;
        }
        else {
            return false;
        }
    }

    void print_value(std::ostream& os) const final { os << dest_; }
};

//! specialization of argument for double options or parameters
struct CmdlineParser::ArgumentDouble final : public Argument {
    double& dest_;

    //! contructor filling most attributes
    ArgumentDouble(char key, const std::string& longkey,
                   const std::string& keytype, const std::string& desc,
                   bool required, double& dest) // NOLINT
        : Argument(key, longkey, keytype, desc, required), dest_(dest) { }

    const char * type_name() const final { return "double"; }

    //! parse unsigned integer using sscanf.
    bool process(int& argc, const char* const*& argv) final { // NOLINT
        if (argc == 0)
            return false;
        if (sscanf(argv[0], "%lf", &dest_) == 1) {
            --argc, ++argv;
            return true;
        }
        else {
            return false;
        }
    }

    void print_value(std::ostream& os) const final { os << dest_; }
};

//! specialization of argument for SI/IEC suffixes byte size options or
//! parameters
struct CmdlineParser::ArgumentBytes32 final : public Argument {
    uint32_t& dest_;

    //! contructor filling most attributes
    ArgumentBytes32(char key, const std::string& longkey,
                    const std::string& keytype, const std::string& desc,
                    bool required, uint32_t& dest) // NOLINT
        : Argument(key, longkey, keytype, desc, required), dest_(dest) { }

    const char * type_name() const final { return "bytes"; }

    //! parse byte size using SI/IEC parser.
    bool process(int& argc, const char* const*& argv) final { // NOLINT
        if (argc == 0)
            return false;
        uint64_t dest;
        if (parse_si_iec_units(argv[0], &dest) &&
            (uint64_t)(dest_ = (uint32_t)dest) == dest) {
            --argc, ++argv;
            return true;
        }
        else {
            return false;
        }
    }

    void print_value(std::ostream& os) const final { os << dest_; }
};

//! specialization of argument for SI/IEC suffixes byte size options or
//! parameters
struct CmdlineParser::ArgumentBytes64 final : public Argument {
    uint64_t& dest_;

    //! contructor filling most attributes
    ArgumentBytes64(char key, const std::string& longkey,
                    const std::string& keytype, const std::string& desc,
                    bool required, uint64_t& dest) // NOLINT
        : Argument(key, longkey, keytype, desc, required), dest_(dest) { }

    const char * type_name() const final { return "bytes"; }

    //! parse byte size using SI/IEC parser.
    bool process(int& argc, const char* const*& argv) final { // NOLINT
        if (argc == 0)
            return false;
        if (parse_si_iec_units(argv[0], &dest_)) {
            --argc, ++argv;
            return true;
        }
        else {
            return false;
        }
    }

    void print_value(std::ostream& os) const final { os << dest_; }
};

//! specialization of argument for string options or parameters
struct CmdlineParser::ArgumentString final : public Argument {
    std::string& dest_;

    //! contructor filling most attributes
    ArgumentString(char key, const std::string& longkey,
                   const std::string& keytype, const std::string& desc,
                   bool required, std::string& dest) // NOLINT
        : Argument(key, longkey, keytype, desc, required), dest_(dest) { }

    const char * type_name() const final { return "string"; }

    //! "process" string argument just by storing it.
    bool process(int& argc, const char* const*& argv) final { // NOLINT
        if (argc == 0)
            return false;
        dest_ = argv[0];
        --argc, ++argv;
        return true;
    }

    void print_value(std::ostream& os) const final {
        os << '"' << dest_ << '"';
    }
};

//! specialization of argument for multiple string options or parameters
struct CmdlineParser::ArgumentStringlist final : public Argument {
    std::vector<std::string>& dest_;

    //! contructor filling most attributes
    ArgumentStringlist(char key, const std::string& longkey,
                       const std::string& keytype, const std::string& desc,
                       bool required, std::vector<std::string>& dest) // NOLINT
        : Argument(key, longkey, keytype, desc, required), dest_(dest) {
        repeated_ = true;
    }

    const char * type_name() const final { return "string list"; }

    //! "process" string argument just by storing it in vector.
    bool process(int& argc, const char* const*& argv) final { // NOLINT
        if (argc == 0)
            return false;
        dest_.push_back(argv[0]);
        --argc, ++argv;
        return true;
    }

    void print_value(std::ostream& os) const final {
        os << '[';
        for (size_t i = 0; i < dest_.size(); ++i) {
            if (i != 0)
                os << ',';
            os << '"' << dest_[i] << '"';
        }
        os << ']';
    }
};

/******************************************************************************/

void CmdlineParser::calc_option_max(const Argument* arg) {
    option_max_width_ = std::max(
        static_cast<int>(arg->option_text().size() + 2), option_max_width_);
}

void CmdlineParser::calc_param_max(const Argument* arg) {
    param_max_width_ = std::max(
        static_cast<int>(arg->param_text().size() + 2), param_max_width_);
}

/******************************************************************************/

void CmdlineParser::output_wrap(
    std::ostream& os, const std::string& text,
    size_t wraplen, size_t indent_first, size_t indent_rest, size_t current,
    size_t indent_newline) {

    std::string::size_type t = 0;
    size_t indent = indent_first;

    while (t != text.size()) {
        std::string::size_type to = t, lspace = t;

        // scan forward in text until we hit a newline or wrap point
        while (to != text.size() && to + current + indent < t + wraplen &&
               text[to] != '\n') {
            if (text[to] == ' ')
                lspace = to;
            ++to;
        }

        // go back to last space
        if (to != text.size() && text[to] != '\n' && lspace != t)
            to = lspace + 1;

        // output line
        os << std::string(indent, ' ') << text.substr(t, to - t) << std::endl;

        current = 0;
        indent = indent_rest;

        // skip over last newline
        if (to != text.size() && text[to] == '\n') {
            indent = indent_newline;
            ++to;
        }

        t = to;
    }
}

/******************************************************************************/

CmdlineParser::CmdlineParser() { }

CmdlineParser::~CmdlineParser() {
    for (size_t i = 0; i < option_list_.size(); ++i)
        delete option_list_[i];
    option_list_.clear();

    for (size_t i = 0; i < param_list_.size(); ++i)
        delete param_list_[i];
    param_list_.clear();
}

void CmdlineParser::set_description(const std::string& description) {
    description_ = description;
}

void CmdlineParser::set_author(const std::string& author) {
    author_ = author;
}

void CmdlineParser::set_verbose_process(bool verbose_process) {
    verbose_process_ = verbose_process;
}

/******************************************************************************/

void CmdlineParser::add_bool(char key, const std::string& longkey,
                             const std::string& keytype, bool& dest,
                             const std::string& desc) {
    option_list_.push_back(
        new ArgumentBool(key, longkey, keytype, desc, false, dest));
    calc_option_max(option_list_.back());
}

void CmdlineParser::add_flag(char key, const std::string& longkey,
                             const std::string& keytype, bool& dest,
                             const std::string& desc) {
    return add_bool(key, longkey, keytype, dest, desc);
}

void CmdlineParser::add_int(char key, const std::string& longkey,
                            const std::string& keytype, int& dest,
                            const std::string& desc) {
    option_list_.push_back(
        new ArgumentInt(key, longkey, keytype, desc, false, dest));
    calc_option_max(option_list_.back());
}

void CmdlineParser::add_unsigned(char key, const std::string& longkey,
                                 const std::string& keytype, unsigned int& dest,
                                 const std::string& desc) {
    option_list_.push_back(
        new ArgumentUnsigned(key, longkey, keytype, desc, false, dest));
    calc_option_max(option_list_.back());
}

void CmdlineParser::add_uint(char key, const std::string& longkey,
                             const std::string& keytype, unsigned int& dest,
                             const std::string& desc) {
    return add_unsigned(key, longkey, keytype, dest, desc);
}

void CmdlineParser::add_size_t(char key, const std::string& longkey,
                               const std::string& keytype, size_t& dest,
                               const std::string& desc) {
    option_list_.push_back(
        new ArgumentSizeT(key, longkey, keytype, desc, false, dest));
    calc_option_max(option_list_.back());
}

void CmdlineParser::add_float(char key, const std::string& longkey,
                              const std::string& keytype, float& dest,
                              const std::string& desc) {
    option_list_.push_back(
        new ArgumentFloat(key, longkey, keytype, desc, false, dest));
    calc_option_max(option_list_.back());
}

void CmdlineParser::add_double(char key, const std::string& longkey,
                               const std::string& keytype, double& dest,
                               const std::string& desc) {
    option_list_.push_back(
        new ArgumentDouble(key, longkey, keytype, desc, false, dest));
    calc_option_max(option_list_.back());
}

void CmdlineParser::add_bytes(char key, const std::string& longkey,
                              const std::string& keytype, uint32_t& dest,
                              const std::string& desc) {
    option_list_.push_back(
        new ArgumentBytes32(key, longkey, keytype, desc, false, dest));
    calc_option_max(option_list_.back());
}

void CmdlineParser::add_bytes(char key, const std::string& longkey,
                              const std::string& keytype, uint64_t& dest,
                              const std::string& desc) {
    option_list_.push_back(
        new ArgumentBytes64(key, longkey, keytype, desc, false, dest));
    calc_option_max(option_list_.back());
}

void CmdlineParser::add_string(char key, const std::string& longkey,
                               const std::string& keytype, std::string& dest,
                               const std::string& desc) {
    option_list_.push_back(
        new ArgumentString(key, longkey, keytype, desc, false, dest));
    calc_option_max(option_list_.back());
}

void CmdlineParser::add_stringlist(
    char key, const std::string& longkey,
    const std::string& keytype, std::vector<std::string>& dest,
    const std::string& desc) {

    option_list_.push_back(
        new ArgumentStringlist(key, longkey, keytype, desc, false, dest));
    calc_option_max(option_list_.back());
}

/******************************************************************************/

void CmdlineParser::add_bool(
    char key, const std::string& longkey, bool& dest, const std::string& desc) {
    return add_bool(key, longkey, "", dest, desc);
}

void CmdlineParser::add_flag(
    char key, const std::string& longkey, bool& dest, const std::string& desc) {
    return add_bool(key, longkey, dest, desc);
}

void CmdlineParser::add_int(
    char key, const std::string& longkey, int& dest, const std::string& desc) {
    return add_int(key, longkey, "", dest, desc);
}

void CmdlineParser::add_unsigned(char key, const std::string& longkey,
                                 unsigned int& dest, const std::string& desc) {
    return add_unsigned(key, longkey, "", dest, desc);
}

void CmdlineParser::add_uint(char key, const std::string& longkey,
                             unsigned int& dest, const std::string& desc) {
    return add_unsigned(key, longkey, dest, desc);
}

void CmdlineParser::add_size_t(char key, const std::string& longkey,
                               size_t& dest, const std::string& desc) {
    return add_size_t(key, longkey, "", dest, desc);
}

void CmdlineParser::add_float(char key, const std::string& longkey,
                              float& dest, const std::string& desc) {
    return add_float(key, longkey, "", dest, desc);
}

void CmdlineParser::add_double(char key, const std::string& longkey,
                               double& dest, const std::string& desc) {
    return add_double(key, longkey, "", dest, desc);
}

void CmdlineParser::add_bytes(char key, const std::string& longkey,
                              uint32_t& dest, const std::string& desc) {
    return add_bytes(key, longkey, "", dest, desc);
}

void CmdlineParser::add_bytes(char key, const std::string& longkey,
                              uint64_t& dest, const std::string& desc) {
    return add_bytes(key, longkey, "", dest, desc);
}

void CmdlineParser::add_string(char key, const std::string& longkey,
                               std::string& dest, const std::string& desc) {
    return add_string(key, longkey, "", dest, desc);
}

void CmdlineParser::add_stringlist(
    char key, const std::string& longkey,
    std::vector<std::string>& dest, const std::string& desc) {
    return add_stringlist(key, longkey, "", dest, desc);
}

/******************************************************************************/

void CmdlineParser::add_param_int(
    const std::string& name, int& dest, const std::string& desc) {
    param_list_.push_back(new ArgumentInt(0, name, "", desc, true, dest));
    calc_param_max(param_list_.back());
}

void CmdlineParser::add_param_unsigned(
    const std::string& name, unsigned int& dest, const std::string& desc) {
    param_list_.push_back(new ArgumentUnsigned(0, name, "", desc, true, dest));
    calc_param_max(param_list_.back());
}

void CmdlineParser::add_param_uint(
    const std::string& name, unsigned int& dest, const std::string& desc) {
    add_param_unsigned(name, dest, desc);
}

void CmdlineParser::add_param_size_t(
    const std::string& name, size_t& dest, const std::string& desc) {
    param_list_.push_back(new ArgumentSizeT(0, name, "", desc, true, dest));
    calc_param_max(param_list_.back());
}

void CmdlineParser::add_param_float(
    const std::string& name, float& dest, const std::string& desc) {
    param_list_.push_back(new ArgumentFloat(0, name, "", desc, true, dest));
    calc_param_max(param_list_.back());
}

void CmdlineParser::add_param_double(
    const std::string& name, double& dest, const std::string& desc) {
    param_list_.push_back(new ArgumentDouble(0, name, "", desc, true, dest));
    calc_param_max(param_list_.back());
}

void CmdlineParser::add_param_bytes(
    const std::string& name, uint32_t& dest, const std::string& desc) {
    param_list_.push_back(new ArgumentBytes32(0, name, "", desc, true, dest));
    calc_param_max(param_list_.back());
}

void CmdlineParser::add_param_bytes(
    const std::string& name, uint64_t& dest, const std::string& desc) {
    param_list_.push_back(new ArgumentBytes64(0, name, "", desc, true, dest));
    calc_param_max(param_list_.back());
}

void CmdlineParser::add_param_string(
    const std::string& name, std::string& dest, const std::string& desc) {
    param_list_.push_back(new ArgumentString(0, name, "", desc, true, dest));
    calc_param_max(param_list_.back());
}

void CmdlineParser::add_param_stringlist(
    const std::string& name, std::vector<std::string>& dest,
    const std::string& desc) {
    param_list_.push_back(
        new ArgumentStringlist(0, name, "", desc, true, dest));
    calc_param_max(param_list_.back());
}

/******************************************************************************/

void CmdlineParser::add_opt_param_int(
    const std::string& name, int& dest, const std::string& desc) {
    param_list_.push_back(new ArgumentInt(0, name, "", desc, false, dest));
    calc_param_max(param_list_.back());
}

void CmdlineParser::add_opt_param_unsigned(
    const std::string& name, unsigned int& dest, const std::string& desc) {
    param_list_.push_back(new ArgumentUnsigned(0, name, "", desc, false, dest));
    calc_param_max(param_list_.back());
}

void CmdlineParser::add_opt_param_uint(
    const std::string& name, unsigned int& dest, const std::string& desc) {
    return add_opt_param_unsigned(name, dest, desc);
}

void CmdlineParser::add_opt_param_size_t(
    const std::string& name, size_t& dest, const std::string& desc) {
    param_list_.push_back(new ArgumentSizeT(0, name, "", desc, false, dest));
    calc_param_max(param_list_.back());
}

void CmdlineParser::add_opt_param_float(
    const std::string& name, float& dest, const std::string& desc) {
    param_list_.push_back(new ArgumentFloat(0, name, "", desc, false, dest));
    calc_param_max(param_list_.back());
}

void CmdlineParser::add_opt_param_double(
    const std::string& name, double& dest, const std::string& desc) {
    param_list_.push_back(new ArgumentDouble(0, name, "", desc, false, dest));
    calc_param_max(param_list_.back());
}

void CmdlineParser::add_opt_param_bytes(
    const std::string& name, uint32_t& dest, const std::string& desc) {
    param_list_.push_back(new ArgumentBytes32(0, name, "", desc, false, dest));
    calc_param_max(param_list_.back());
}

void CmdlineParser::add_opt_param_bytes(
    const std::string& name, uint64_t& dest, const std::string& desc) {
    param_list_.push_back(new ArgumentBytes64(0, name, "", desc, false, dest));
    calc_param_max(param_list_.back());
}

void CmdlineParser::add_opt_param_string(
    const std::string& name, std::string& dest, const std::string& desc) {
    param_list_.push_back(new ArgumentString(0, name, "", desc, false, dest));
    calc_param_max(param_list_.back());
}

void CmdlineParser::add_opt_param_stringlist(
    const std::string& name, std::vector<std::string>& dest,
    const std::string& desc) {
    param_list_.push_back(
        new ArgumentStringlist(0, name, "", desc, false, dest));
    calc_param_max(param_list_.back());
}

/******************************************************************************/

void CmdlineParser::print_usage(std::ostream& os) {
    std::ios::fmtflags flags(os.flags());

    os << "Usage: " << program_name_
       << (option_list_.size() ? " [options]" : "");

    for (ArgumentList::const_iterator it = param_list_.begin();
         it != param_list_.end(); ++it) {
        const Argument* arg = *it;

        os << (arg->required_ ? " <" : " [") << arg->longkey_
           << (arg->repeated_ ? " ..." : "") << (arg->required_ ? '>' : ']');
    }

    os << std::endl;

    if (description_.size()) {
        os << std::endl;
        output_wrap(os, description_, line_wrap_);
    }
    if (author_.size()) {
        os << "Author: " << author_ << std::endl;
    }

    if (description_.size() || author_.size())
        os << std::endl;

    if (param_list_.size()) {
        os << "Parameters:" << std::endl;

        for (ArgumentList::const_iterator it = param_list_.begin();
             it != param_list_.end(); ++it) {
            const Argument* arg = *it;

            os << "  " << std::setw(param_max_width_) << std::left
               << arg->param_text();
            output_wrap(os, arg->desc_, line_wrap_, 0, param_max_width_ + 2,
                        param_max_width_ + 2, 8);
        }
    }

    if (option_list_.size()) {
        os << "Options:" << std::endl;

        for (ArgumentList::const_iterator it = option_list_.begin();
             it != option_list_.end(); ++it) {
            const Argument* arg = *it;

            os << "  " << std::setw(option_max_width_) << std::left
               << arg->option_text();
            output_wrap(os, arg->desc_, line_wrap_, 0, option_max_width_ + 2,
                        option_max_width_ + 2, 8);
        }
    }

    os.flags(flags);
}

void CmdlineParser::print_usage() {
    return print_usage(std::cout);
}

void CmdlineParser::print_option_error(
    int argc, const char* const* argv, const Argument* arg, std::ostream& os) {
    os << "Error: argument ";
    if (argc != 0)
        os << '"' << argv[0] << '"';

    os << " for " << arg->type_name() << " option " << arg->option_text()
       << (argc == 0 ? " is missing!" : " is invalid!") << std::endl
       << std::endl;

    print_usage(os);
}

void CmdlineParser::print_param_error(
    int argc, const char* const* argv, const Argument* arg, std::ostream& os) {
    os << "Error: argument ";
    if (argc != 0)
        os << '"' << argv[0] << '"';

    os << " for " << arg->type_name() << " parameter " << arg->param_text()
       << (argc == 0 ? " is missing!" : " is invalid!") << std::endl
       << std::endl;

    print_usage(os);
}

bool CmdlineParser::process(
    int argc, const char* const* argv, std::ostream& os) {
    program_name_ = argv[0];
    --argc, ++argv;

    // search for help string and output help
    for (int i = 0; i < argc; ++i) {
        if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            print_usage(os);
            return false;
        }
    }

    // current argument in param_list_
    ArgumentList::iterator argi = param_list_.begin();
    bool end_optlist = false;

    while (argc != 0) {
        const char* arg = argv[0];

        if (arg[0] == '-' && !end_optlist) {
            // option, advance to argument
            --argc, ++argv;
            if (arg[1] == '-') {
                if (arg[2] == '-') {
                    end_optlist = true;
                }
                else {
                    // long option
                    ArgumentList::const_iterator oi = option_list_.begin();
                    for ( ; oi != option_list_.end(); ++oi) {
                        if ((arg + 2) == (*oi)->longkey_) {
                            if (!(*oi)->process(argc, argv)) {
                                print_option_error(argc, argv, *oi, os);
                                return false;
                            }
                            else if (verbose_process_) {
                                os << "Option " << (*oi)->option_text()
                                   << " set to ";
                                (*oi)->print_value(os);
                                os << '.' << std::endl;
                            }
                            break;
                        }
                    }
                    if (oi == option_list_.end()) {
                        os << "Error: unknown option \"" << arg << "\"."
                           << std::endl << std::endl;
                        print_usage(os);
                        return false;
                    }
                }
            }
            else {
                // short option
                if (arg[1] == 0) {
                    os << "Invalid option \"" << arg << "\"." << std::endl;
                }
                else {
                    size_t offset = 1, arg_length = strlen(arg);
                    int old_argc = argc;
                    // Arguments will increase argc, so abort if it increases,
                    // while flags won't, so increase offset and parse next
                    while (offset < arg_length && argc == old_argc) {
                        ArgumentList::const_iterator oi = option_list_.begin();
                        for ( ; oi != option_list_.end(); ++oi) {
                            if (arg[offset] == (*oi)->key_) {
                                ++offset;
                                if (!(*oi)->process(argc, argv)) {
                                    print_option_error(argc, argv, *oi, os);
                                    return false;
                                }
                                else if (verbose_process_) {
                                    os << "Option "
                                       << (*oi)->option_text()
                                       << " set to ";
                                    (*oi)->print_value(os);
                                    os << '.' << std::endl;
                                }
                                break;
                            }
                        }
                        if (oi == option_list_.end()) {
                            os << "Error: unknown option \"";
                            if (arg_length > 2) {
                                // multiple short options combined
                                os << "-" << arg[offset]
                                   << "\" at position " << offset
                                   << " in option sequence \"";
                            }
                            os << arg << "\"." << std::endl << std::endl;
                            print_usage(os);
                            return false;
                        }
                    }
                }
            }
        }
        else {
            if (argi != param_list_.end()) {
                if (!(*argi)->process(argc, argv)) {
                    print_param_error(argc, argv, *argi, os);
                    return false;
                }
                else if (verbose_process_) {
                    os << "Parameter " << (*argi)->param_text() << " set to ";
                    (*argi)->print_value(os);
                    os << '.' << std::endl;
                }
                (*argi)->found_ = true;
                if (!(*argi)->repeated_)
                    ++argi;
            }
            else {
                os << "Error: unexpected extra argument "
                   << "\"" << argv[0] << "\"." << std::endl << std::endl;
                --argc, ++argv;
                print_usage(os);
                return false;
            }
        }
    }

    bool good = true;

    for (ArgumentList::const_iterator it = param_list_.begin();
         it != param_list_.end(); ++it) {
        if ((*it)->required_ && !(*it)->found_) {
            os << "Error: argument for parameter " << (*it)->longkey_
               << " is required!" << std::endl;
            good = false;
        }
    }

    if (!good) {
        os << std::endl;
        print_usage(os);
    }

    return good;
}

bool CmdlineParser::process(int argc, const char* const* argv) {
    return process(argc, argv, std::cout);
}

void CmdlineParser::print_result(std::ostream& os) {
    std::ios::fmtflags flags(os.flags());

    int maxlong = std::max(param_max_width_, option_max_width_);

    if (param_list_.size()) {
        os << "Parameters:" << std::endl;

        for (ArgumentList::const_iterator it = param_list_.begin();
             it != param_list_.end(); ++it) {
            const Argument* arg = *it;

            os << "  " << std::setw(maxlong) << std::left << arg->param_text();

            std::string typestr = "(" + std::string(arg->type_name()) + ")";
            os << std::setw(max_type_name_ + 4) << typestr;

            arg->print_value(os);

            os << std::endl;
        }
    }

    if (option_list_.size()) {
        os << "Options:" << std::endl;

        for (ArgumentList::const_iterator it = option_list_.begin();
             it != option_list_.end(); ++it) {
            const Argument* arg = *it;

            os << "  " << std::setw(maxlong) << std::left << arg->option_text();

            std::string typestr = "(" + std::string(arg->type_name()) + ")";
            os << std::setw(max_type_name_ + 4) << std::left << typestr;

            arg->print_value(os);

            os << std::endl;
        }
    }

    os.flags(flags);
}

void CmdlineParser::print_result() {
    return print_result(std::cout);
}

} // namespace tlx

/******************************************************************************/