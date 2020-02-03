#pragma once
#include <cinttypes>
#include <sstream>
#include <regex>

struct rmat_args
{
    double a, b, c, d;
    int64_t num_edges, num_vertices;

    static int64_t
    parse_int_with_suffix(std::string token)
    {
        int64_t n = static_cast<int64_t>(std::stoll(token));
        switch(token.back())
        {
            case 'K': n *= 1LL << 10; break;
            case 'M': n *= 1LL << 20; break;
            case 'G': n *= 1LL << 30; break;
            case 'T': n *= 1LL << 40; break;
            default: break;
        }
        return n;
    }

    static rmat_args
    from_string(const std::string& str)
    {
        rmat_args args;
        std::smatch m;

        std::regex r1(R"((\d+[.]\d+)-(\d+[.]\d+)-(\d+[.]\d+)-(\d+[.]\d+)-(\d+[KMGT]?)-(\d+[KMGT]?)\.rmat)");
        std::regex r2(R"(graph500-scale(\d+)(\.mtx)?)");

        if (std::regex_match(str, m, r1)) {
            args.a = std::stod(m[1]);
            args.b = std::stod(m[2]);
            args.c = std::stod(m[3]);
            args.d = std::stod(m[4]);
            args.num_edges = parse_int_with_suffix(m[5]);
            args.num_vertices = parse_int_with_suffix(m[6]);
        } else if (std::regex_match(str, m, r2)) {
            args.a = 0.57;
            args.b = 0.19;
            args.c = 0.19;
            args.d = 0.05;
            auto scale = parse_int_with_suffix(m[1]);
            args.num_vertices = (1 << scale);
            args.num_edges = 16 * args.num_vertices;
        } else {
            // Raise an error here?
        }

        return args;
    }

    std::string
    validate() const {
        std::ostringstream oss;
        // Validate parameters
        if (a < 0 || b < 0 || c < 0 || d < 0
        ||  a > 1 || b > 1 || c > 1 || d > 1
        ||  a + b + c + d != 1.0)
        {
            oss << "Invalid arguments: RMAT parameters must be fall in the range [0, 1] and sum to 1\n";
        } else if (num_edges < 0 || num_vertices < 0) {
            oss << "Invalid arguments: RMAT graph must have a positive number of edges and vertices\n";
        }
        return oss.str();
    }
};
