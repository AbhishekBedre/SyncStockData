﻿using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Internal;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using OptionChain;
using Quartz;
using SyncData;

class Program
{
    static async Task Main(string[] args)
    {
        const string SESSION_EXPRESSION = "0 0 9-16 ? * MON-FRI";
        const string FIRST_SESSION_EXP = "0 15-59/5 9 ? * MON-FRI";
        const string MID_SESSION_EXP = "0 0-59/5 10-14 ? * MON-FRI";
        const string LAST_SESSION_EXP = "0 0-30/5 15 ? * MON-FRI";
        const string FINAL_SESSION_EXP = "0 0 16 ? * MON-FRI";

        var host = Host.CreateDefaultBuilder(args)
            .ConfigureServices((hostContext, services) =>
            {
                string connectionString = hostContext.Configuration.GetSection("ConnectionStrings:DefaultConnection").Value.ToString();

                services.AddDbContext<OptionDbContext>(x => x.UseSqlServer(connectionString));

                var alwaysLive = JobKey.Create("alwaysLive");

                // Add Quartz services
                services.AddQuartz(q =>
                {
                    #region "SESSION UPDATE JOB"

                    var sessionUpdateJob = JobKey.Create("sessionUpdateJob");                    

                    // 0 0 9-15 ? * MON-FRI
                    q.AddJob<SessionUpdateJob>(sessionUpdateJob)
                        .AddTrigger(trigger =>
                        {
                            //trigger.ForJob(sessionUpdateJob).WithSimpleSchedule(s => s.WithIntervalInMinutes(2)); 
                            trigger.ForJob(sessionUpdateJob).WithCronSchedule(SESSION_EXPRESSION); 
                        });

                    #endregion

                    #region "BANK NIFTY UPDATE JOB"

                    var bankNiftyFirstSession = JobKey.Create("bankNiftyFirstSession");

                    q.AddJob<BankNiftyUpdateJob>(bankNiftyFirstSession)
                        .AddTrigger(trigger =>
                        {
                            trigger.ForJob(bankNiftyFirstSession).WithCronSchedule(FIRST_SESSION_EXP);
                            //trigger.ForJob(bankNiftyFirstSession).WithSimpleSchedule(x=>x.WithIntervalInMinutes(2));
                        });

                    var bankNiftyMidSession = JobKey.Create("bankNiftyMidSession");

                    q.AddJob<BankNiftyUpdateJob>(bankNiftyMidSession)
                        .AddTrigger(trigger =>
                        {
                            trigger.ForJob(bankNiftyMidSession).WithCronSchedule(MID_SESSION_EXP); // From 10:00 AM to 2:59 PM, Monday to Friday
                        });

                    var bankNiftylastSession = JobKey.Create("bankNiftylastSession");

                    q.AddJob<BankNiftyUpdateJob>(bankNiftylastSession)
                        .AddTrigger(trigger =>
                        {
                            trigger.ForJob(bankNiftylastSession).WithCronSchedule(LAST_SESSION_EXP); // From 3:00 PM to 3:30 PM, Monday to Friday
                        });

                    var bankNiftyFinalCall = JobKey.Create("bankNiftyFinalCall");

                    q.AddJob<BankNiftyUpdateJob>(bankNiftyFinalCall)
                        .AddTrigger(trigger =>
                        {
                            trigger.ForJob(bankNiftyFinalCall).WithCronSchedule(FINAL_SESSION_EXP); // At 4:00 PM, Monday to Friday
                        });

                    #endregion
                    
                    #region "NIFTY UPDATE JOB"

                    var niftyFirstSession = JobKey.Create("niftyFirstSession");

                    q.AddJob<NiftyUpdateJob>(niftyFirstSession)
                        .AddTrigger(trigger =>
                        {
                            //trigger.ForJob(niftyFirstSession).WithSimpleSchedule(x=>x.WithIntervalInMinutes(5));
                            trigger.ForJob(niftyFirstSession).WithCronSchedule(FIRST_SESSION_EXP);
                        });

                    var niftyMidSession = JobKey.Create("niftyMidSession");

                    q.AddJob<NiftyUpdateJob>(niftyMidSession)
                        .AddTrigger(trigger =>
                        {
                            trigger.ForJob(niftyMidSession).WithCronSchedule(MID_SESSION_EXP); // From 10:00 AM to 2:59 PM, Monday to Friday
                        });

                    var niftylastSession = JobKey.Create("niftylastSession");

                    q.AddJob<NiftyUpdateJob>(niftylastSession)
                        .AddTrigger(trigger =>
                        {
                            trigger.ForJob(niftylastSession).WithCronSchedule(LAST_SESSION_EXP); // From 3:00 PM to 3:30 PM, Monday to Friday
                        });

                    var niftyFinalCall = JobKey.Create("niftyFinalCall");

                    q.AddJob<NiftyUpdateJob>(niftyFinalCall)
                        .AddTrigger(trigger =>
                        {
                            trigger.ForJob(niftyFinalCall).WithCronSchedule(FINAL_SESSION_EXP); // At 4:00 PM, Monday to Friday
                        });

                    #endregion

                    #region "STOCK UPDATE JOB"

                    var stockFirstSession = JobKey.Create("stockFirstSession");

                    q.AddJob<StocksUpdateJob>(stockFirstSession)
                        .AddTrigger(trigger =>
                        {
                            //trigger.ForJob(stockFirstSession).WithSimpleSchedule(s => s.WithIntervalInMinutes(2));
                            trigger.ForJob(stockFirstSession).WithCronSchedule(FIRST_SESSION_EXP);
                        });

                    var stockMidSession = JobKey.Create("stockMidSession");

                    q.AddJob<StocksUpdateJob>(stockMidSession)
                        .AddTrigger(trigger =>
                        {
                            trigger.ForJob(stockMidSession).WithCronSchedule(MID_SESSION_EXP); // From 10:00 AM to 2:59 PM, Monday to Friday
                        });

                    var stocklastSession = JobKey.Create("stocklastSession");

                    q.AddJob<StocksUpdateJob>(stocklastSession)
                        .AddTrigger(trigger =>
                        {
                            trigger.ForJob(stocklastSession).WithCronSchedule(LAST_SESSION_EXP); // From 3:00 PM to 3:30 PM, Monday to Friday
                        });

                    var stockFinalCall = JobKey.Create("stockFinalCall");

                    q.AddJob<StocksUpdateJob>(stockFinalCall)
                        .AddTrigger(trigger =>
                        {
                            trigger.ForJob(stockFinalCall).WithCronSchedule(FINAL_SESSION_EXP); // At 4:00 PM, Monday to Friday
                            //trigger.ForJob(stockFinalCall).WithSimpleSchedule(x=>x.WithIntervalInMinutes(10)); // At 4:00 PM, Monday to Friday
                        });

                    #endregion

                    #region "INDEX UPDATE JOB"

                    var indexFirstSession = JobKey.Create("indexFirstSession");

                    q.AddJob<BroderMarketsUpdateJob>(indexFirstSession)
                        .AddTrigger(trigger =>
                        {
                            trigger.ForJob(indexFirstSession).WithCronSchedule(FIRST_SESSION_EXP);
                            //trigger.ForJob(indexFirstSession).WithSimpleSchedule(s => s.WithIntervalInMinutes(5)); 
                        });

                    var indexMidSession = JobKey.Create("indexMidSession");

                    q.AddJob<BroderMarketsUpdateJob>(indexMidSession)
                        .AddTrigger(trigger =>
                        {
                            trigger.ForJob(indexMidSession).WithCronSchedule(MID_SESSION_EXP); // From 10:00 AM to 2:59 PM, Monday to Friday
                        });

                    var indexlastSession = JobKey.Create("indexlastSession");

                    q.AddJob<BroderMarketsUpdateJob>(indexlastSession)
                        .AddTrigger(trigger =>
                        {
                            trigger.ForJob(indexlastSession).WithCronSchedule(LAST_SESSION_EXP); // From 3:00 PM to 3:30 PM, Monday to Friday
                        });

                    var indexFinalCall = JobKey.Create("indexFinalCall");

                    q.AddJob<BroderMarketsUpdateJob>(indexFinalCall)
                        .AddTrigger(trigger =>
                        {
                            trigger.ForJob(indexFinalCall).WithCronSchedule(FINAL_SESSION_EXP); // At 4:00 PM, Monday to Friday
                        });

                    #endregion

                });

                // Add Quartz hosted service with WaitForJobsToComplete
                services.AddQuartzHostedService(options =>
                {
                    options.WaitForJobsToComplete = true;
                });

            }).Build();

        await host.RunAsync();
    }
}
