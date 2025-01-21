﻿using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using OptionChain;
using Quartz;
using System.Text.Json;

namespace SyncData
{
    public class StocksUpdateJob : IJob
    {
        private readonly ILogger<StocksUpdateJob> _logger;
        private readonly OptionDbContext _optionDbContext;
        private object counter = 0;
        private object stockCounter = 0;
        private double? previousCPEOIDiffValue = null; // To store the previous X value
        private double? previousCPEColDiffValue = null; // To store the previous X value

        public StocksUpdateJob(ILogger<StocksUpdateJob> log, OptionDbContext optionDbContext)
        {
            _logger = log;
            _optionDbContext = optionDbContext;
        }

        public async Task Execute(IJobExecutionContext context)
        {
            _logger.LogInformation($"{nameof(StocksUpdateJob)} Started: " + context.FireTimeUtc.ToLocalTime().ToString("hh:mm:ss"));
            Utility.LogDetails($"{nameof(StocksUpdateJob)} Started: " + context.FireTimeUtc.ToLocalTime().ToString("hh:mm:ss"));

            await GetStockData(context);

            await Task.CompletedTask;
        }

        public async Task GetStockData(IJobExecutionContext context)
        {

        STEP:

            try
            {
                (bool status, object result, StockRoot StockRoot) = await GetStockData(stockCounter, context);

                if (status == false && Convert.ToInt16(result) <= 3)
                {
                    await Task.Delay(2000);
                    stockCounter = result;

                    goto STEP;
                }

                if (Convert.ToInt32(stockCounter) <= 3)
                {
                    stockCounter = 0;
                    // Make a Db Call                    
                    await StoreStockDataInTable(StockRoot, context);
                }
            }
            catch (Exception ex)
            {
                _logger.LogInformation($"Multiple tried for stock data but not succeed. counter: {stockCounter}");
                stockCounter = 0;

                Utility.LogDetails($"{nameof(GetStockData)} Exception: {ex.Message}");
            }
        }

        private async Task<(bool, object, StockRoot)> GetStockData(object counter, IJobExecutionContext context)
        {
            Utility.LogDetails($"{nameof(GetStockData)} -> Send quots reqest counter:" + counter + ", Time: " + context.FireTimeUtc.ToLocalTime().ToString("hh:mm"));

            bool status = true;
            StockRoot stockData = null;
            string sessionCookie = "";

            _logger.LogInformation($"Exection time: {counter}");

            using (HttpClient client = new HttpClient())
            {
                var sessionInfo = await _optionDbContext.Sessions.Where(x => x.Id > 0).FirstOrDefaultAsync();

                if (sessionInfo != null)
                {
                    sessionCookie = sessionInfo.Cookie ?? "";
                }

                client.DefaultRequestHeaders.Add("User-Agent", "PostmanRuntime/7.43.0");
                client.DefaultRequestHeaders.Add("Accept", "*/*");
                client.DefaultRequestHeaders.Add("Connection", "keep-alive");
                client.DefaultRequestHeaders.Add("cookie", sessionCookie);

                string url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20200";

                try
                {
                    HttpResponseMessage response = await client.GetAsync(url);
                    if (response.IsSuccessStatusCode)
                    {
                        string jsonContent = await response.Content.ReadAsStringAsync();

                        var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
                        stockData = JsonSerializer.Deserialize<StockRoot>(jsonContent, options);

                        if (stockData == null || stockData.Data == null)
                        {
                            _logger.LogInformation("Failed to parse JSON content.");
                            Utility.LogDetails($"{nameof(GetStockData)} -> Failed to parse JSON content.");
                            throw new Exception("Failed to parse JSON content.");
                        }
                    }
                    else
                    {
                        Utility.LogDetails($"{nameof(GetStockData)} -> HTTP Error: {response.StatusCode}.");
                        _logger.LogInformation($"HTTP Error: {response.StatusCode}");
                        throw new Exception($"Http Error: {response.StatusCode}");
                    }
                }
                catch (Exception ex)
                {
                    Utility.LogDetails($"{nameof(GetStockData)} -> Exception: {ex.Message}.");
                    _logger.LogInformation($"Exception: {ex.Message}");
                    counter = Convert.ToInt16(counter) + 1;
                    status = false;
                }
            }

            return (status, counter, stockData);
        }

        private async Task<bool> StoreStockDataInTable(StockRoot? stockRoot, IJobExecutionContext context)
        {
            try
            {
                _logger.LogInformation("Adding data to stock table.");

                if (stockRoot != null
                    && stockRoot.Data != null)
                {
                    await _optionDbContext.Database.BeginTransactionAsync();

                    List<StockData> stockDatas = new List<StockData>();
                    List<StockMetaData> stockMetaDatas = new List<StockMetaData>();

                    foreach (var (f, index) in stockRoot.Data.Select((f, index) => (f, index)))
                    {
                        var stockData = new StockData
                        {
                            Priority = f.Priority,
                            Symbol = f.Symbol,
                            Identifier = f.Identifier,
                            Series = f.Series,
                            Open = f.Open,
                            DayHigh = f.DayHigh,
                            DayLow = f.DayLow,
                            LastPrice = f.LastPrice,
                            PreviousClose = f.PreviousClose,
                            Change = f.Change,
                            PChange = f.PChange,
                            TotalTradedVolume = f.TotalTradedVolume,
                            StockIndClosePrice = f.StockIndClosePrice,
                            TotalTradedValue = f.TotalTradedValue,
                            LastUpdateTime = f.LastUpdateTime,
                            YearHigh = f.YearHigh,
                            Ffmc = f.Ffmc,
                            YearLow = f.YearLow,
                            NearWKH = f.NearWKH,
                            NearWKL = f.NearWKL,
                            Date365dAgo = f.Date365dAgo,
                            Chart365dPath = f.Chart365dPath,
                            Date30dAgo = f.Date30dAgo,
                            //PerChange30d = f.PerChange30d,
                            Chart30dPath = f.Chart30dPath,
                            ChartTodayPath = f.ChartTodayPath,
                            EntryDate = DateTime.Now.Date,
                            Time = DateTime.Now.TimeOfDay
                        };

                        stockDatas.Add(stockData);

                        if (f.Meta != null)
                        {
                            var meta = new StockMetaData
                            {
                                Symbol = f.Meta.Symbol,
                                CompanyName = f.Meta.CompanyName,
                                Industry = f.Meta.Industry,
                                IsFNOSec = f.Meta.IsFNOSec,
                                IsCASec = f.Meta.IsCASec,
                                IsSLBSec = f.Meta.IsSLBSec,
                                IsDebtSec = f.Meta.IsDebtSec,
                                IsSuspended = f.Meta.IsSuspended,
                                IsETFSec = f.Meta.IsETFSec,
                                IsDelisted = f.Meta.IsDelisted,
                                Isin = f.Meta.Isin,
                                SlbIsin = f.Meta.SlbIsin,
                                ListingDate = f.Meta.ListingDate,
                                IsMunicipalBond = f.Meta.IsMunicipalBond,
                                EntryDate = DateTime.Now.Date,
                            };

                            stockMetaDatas.Add(meta);
                        }
                    }

                    await _optionDbContext.StockData.AddRangeAsync(stockDatas);

                    // Advances data

                    stockRoot.Advance.EntryDate = DateTime.Now.Date;
                    stockRoot.Advance.Time = context.FireTimeUtc.ToLocalTime().TimeOfDay;

                    await _optionDbContext.Advance.AddRangeAsync(stockRoot.Advance);                    

                    await _optionDbContext.SaveChangesAsync();

                    await _optionDbContext.Database.CommitTransactionAsync();

                    if (context.FireTimeUtc.ToString("hh:mm") == "09:15"
                        || context.FireTimeUtc.ToString("hh:mm") == "09:20"
                        || context.FireTimeUtc.ToString("hh:mm") == "09:25"
                        || context.FireTimeUtc.ToString("hh:mm") == "09:30")
                    {
                        foreach (var stock in stockMetaDatas)
                        {
                            var metaData = await _optionDbContext.StockMetaData.Where(x => x.Symbol.ToLower() == stock.Symbol.ToLower()).FirstOrDefaultAsync();
                            if (metaData == null)
                            {
                                await _optionDbContext.StockMetaData.AddAsync(new StockMetaData
                                {
                                    CompanyName = stock.CompanyName,
                                    IsCASec = stock.IsCASec,
                                    Symbol = stock.Symbol,
                                    EntryDate = DateTime.Now.Date,
                                    Time = context.FireTimeUtc.ToLocalTime().TimeOfDay,
                                    ListingDate = stock.ListingDate,
                                    Industry = stock.Industry,
                                    IsDebtSec = stock.IsDebtSec,
                                    IsDelisted = stock.IsDelisted,
                                    IsETFSec = stock.IsETFSec,
                                    IsFNOSec = stock.IsFNOSec,
                                    Isin = stock.Isin,
                                    IsMunicipalBond = stock.IsMunicipalBond,
                                    IsSLBSec = stock.IsSLBSec,
                                    IsSuspended = stock.IsSuspended,
                                    SlbIsin = stock.SlbIsin,
                                });
                            }
                            else
                            {
                                metaData.CompanyName = stock.CompanyName;
                                metaData.IsCASec = stock.IsCASec;
                                metaData.Symbol = stock.Symbol;
                                metaData.EntryDate = DateTime.Now.Date;
                                metaData.Time = context.FireTimeUtc.ToLocalTime().TimeOfDay;
                                metaData.ListingDate = stock.ListingDate;
                                metaData.Industry = stock.Industry;
                                metaData.IsDebtSec = stock.IsDebtSec;
                                metaData.IsDelisted = stock.IsDelisted;
                                metaData.IsETFSec = stock.IsETFSec;
                                metaData.IsFNOSec = stock.IsFNOSec;
                                metaData.Isin = stock.Isin;
                                metaData.IsMunicipalBond = stock.IsMunicipalBond;
                                metaData.IsSLBSec = stock.IsSLBSec;
                                metaData.IsSuspended = stock.IsSuspended;
                                metaData.SlbIsin = stock.SlbIsin;
                            }

                            await _optionDbContext.SaveChangesAsync();
                        }
                    }

                    // Update the RFactor for all stocks
                    await _optionDbContext.Database.ExecuteSqlRawAsync("EXEC [UpdateRelativeFactor]");
                }
            }
            catch (Exception ex)
            {
                Utility.LogDetails($"{nameof(StoreStockDataInTable)} -> Exception: {ex.Message}.");
                await _optionDbContext.Database.RollbackTransactionAsync();
                return false;
            }

            return true;
        }
    }
}
