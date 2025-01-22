using Microsoft.EntityFrameworkCore;
using OptionChain;
using System.Text.RegularExpressions;

namespace SyncData
{
    public enum JobType
    {
        SessionUpdate,
        NiftyUpdate,
        BroderMarketUpdate,
        StockUpdate,
        BankNiftyUpdate
    }

    public static class Common
    {
        public static async Task UpdateCookieAndHeaders(HttpClient httpClient, OptionDbContext optionDbContext, JobType jobType)
        {
            var sessionCookie = "";

            var sessionInfo = await optionDbContext.Sessions.Where(x => x.Id > 0).FirstOrDefaultAsync();

            if (sessionInfo != null)
            {
                sessionCookie = sessionInfo.Cookie ?? "";
            }

            if(jobType == JobType.BroderMarketUpdate)
            {
                string pattern = @"(nsit=[^;]*;|nseappid=[^;]*;)";

                // Replace matched substrings with an empty string
                sessionCookie = Regex.Replace(sessionCookie, pattern, string.Empty);
            }

            httpClient.DefaultRequestHeaders.Add("User-Agent", "PostmanRuntime/7.43.0");
            httpClient.DefaultRequestHeaders.Add("Accept", "*/*");
            httpClient.DefaultRequestHeaders.Add("Connection", "keep-alive");
            httpClient.DefaultRequestHeaders.Add("cookie", sessionCookie);
        }
    }
}