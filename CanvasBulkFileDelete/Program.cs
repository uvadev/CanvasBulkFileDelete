using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using AppUtils;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Tomlyn.Syntax;
using UVACanvasAccess.ApiParts;
using UVACanvasAccess.Util;

namespace CanvasBulkFileDelete {
    internal static class Program {
        public static void Main() {
            var home = new AppHome("bulk_file_delete");
            
            Console.WriteLine($"Using config path: {home.ConfigPath}");
            
            if (!home.ConfigPresent()) {
                Console.WriteLine("Need to generate a config file.");
                
                home.CreateConfig(new DocumentSyntax {
                    Tables = {
                        new TableSyntax("tokens") {
                            Items = {
                                {"token", "PUT_TOKEN_HERE"}
                            }
                        },
                        new TableSyntax("data") {
                            Items = {
                                {"map_file", "RELATIVE_MAP_FILE_PATH_HERE"},
                                {"id_is_sis", true}
                            }
                        }
                    }
                });

                Console.WriteLine("Created a new config file. Please go put in your token and map info.");
                return;
            }
            
            Console.WriteLine("Found config file.");

            var config = home.GetConfig();
            Debug.Assert(config != null, nameof(config) + " != null");

            var token = config.GetTable("tokens")
                              .Get<string>("token");
            
            var data = config.GetTable("data");
            var mapFileName = data.Get<string>("map_file");
            bool idIsSis = data.GetOr("id_is_sis", true);
            
            string mapFilePath = Path.Combine(home.NsDir, mapFileName);

            Console.WriteLine($"Sourcing map from {mapFilePath}");

            var startedAt = DateTime.Now;
            
            // ------------------------------------------------------------------------
            
            var list = File.ReadAllLines(mapFilePath).ToList();
            
            List<string>[] taskLists = list.Chunk(Math.Min(Math.Max(list.Count / 7, 2), list.Count - 1))
                                           .ToArray();
            
            int nThreads = taskLists.Length;
            
            var apis = new Api[nThreads];
            
            for (var i = 0; i < nThreads; i++) {
                apis[i] = new Api(token, "https://uview.instructure.com/api/v1/");
            }

            Console.WriteLine($"Using {nThreads} threads.");
            
            Console.WriteLine(idIsSis ? "Interpeting userkey as SIS ID." 
                                      : "Interpreting userkey as CANVAS ID.");
            
            var completed = new ConcurrentBag<string>();
            var userNotFound = new ConcurrentBag<string>();
            var fileNotFound = new ConcurrentBag<string>();
            var error = new ConcurrentBag<string>();
            var keys = new ConcurrentBag<string>();
            
            using (var countdown = new CountdownEvent(nThreads)) {
                for (var i = 0; i < nThreads; i++) {
                    ThreadPool.QueueUserWorkItem(async o => {
                        try {
                            var n = (int) o;
                            foreach (string line in taskLists[n]) {
                                string[] halves = line.Split(',');
                                Debug.Assert(halves.Length == 2);

                                var (userKey, userFile) = (halves[0], halves[1]);
                                keys.Add(userKey);

                                var api = apis[n];

                                try {
                                    var user = idIsSis switch {
                                        true => await api.GetUserBySis(userKey),
                                        _    => await api.GetUser(ulong.Parse(userKey))
                                    };

                                    if (user == null) {
                                        Console.WriteLine($"WARN: Couldn't find the user for userkey {userKey} !!");
                                        userNotFound.Add(userKey);
                                        continue;
                                    }

                                    Console.WriteLine($"Preparing to delete filename(s) {userFile} from userkey " +
                                                      $"{userKey}, Id {user.Id}, SIS {user.SisUserId}");

                                    api.MasqueradeAs(user.Id);

                                    var anyDeleted = false;
                                    await foreach (var file in api.StreamPersonalFiles(searchTerm: userFile)) {
                                        if (file.Filename == userFile) {
                                            var deleted = await api.DeleteFile(file.Id, false);
                                            Console.WriteLine($"Deleted {userFile}, id {deleted.Id}, from userkey {userKey}!");
                                            anyDeleted = true;
                                        }
                                    }

                                    if (anyDeleted) {
                                        completed.Add(userKey);
                                    } else {
                                        fileNotFound.Add(userKey);
                                        Console.WriteLine($"Userkey {userKey} has no {userFile} to delete.");
                                    }
                                } catch (Exception e) {
                                    Console.WriteLine($"Caught an exception during upload for userkey {userKey}: {e}");
                                    error.Add(userKey);
                                } finally {
                                    api.StopMasquerading();
                                }
                            }
                        } finally {
                            // ReSharper disable once AccessToDisposedClosure
                            countdown.Signal();
                        }
                    }, i);
                }
                countdown.Wait();
            }

            var completedList = completed.Concat(fileNotFound).ToList();
            var completedWithDeletionIds = completed.Distinct().ToList();
            var completedWithoutDeletionIds = fileNotFound.Distinct().ToList();
            var errorIds = error.Distinct().ToList();
            var userNotFoundIds = userNotFound.Distinct().ToList();

            Console.WriteLine($"{completedList.Count} out of {list.Count} operations were completed.");

            if (errorIds.Any()) {
                Console.WriteLine($"Operation failed for the following userkeys: {errorIds.ToPrettyString()}");
            }

            if (userNotFoundIds.Any()) {
                Console.WriteLine($"The following userkeys could not be resolved: {userNotFoundIds.ToPrettyString()}");
            }

            if (completedWithoutDeletionIds.Any()) {
                Console.WriteLine($"The following userkeys were missing at least one file (usually not an error): {completedWithoutDeletionIds.ToPrettyString()}");
            }

            var document = new JObject {
                ["dateStarted"] = startedAt.ToIso8601Date(),
                ["dateCompleted"] = DateTime.Now.ToIso8601Date(),
                ["completedWithDeletion"] = new JArray(completedWithDeletionIds),
                ["completedWithoutDeletion"] = new JArray(completedWithoutDeletionIds),
                ["error"] = new JArray(errorIds),
                ["userNotFound"] = new JArray(userNotFoundIds)
            };
            
            var outPath = Path.Combine(home.NsDir, $"BulkFileDelete_Log_{startedAt.Ticks}.json");
            File.WriteAllText(outPath, document.ToString(Formatting.Indented) + "\n");
            Console.WriteLine($"Wrote log to {outPath}");
        }
    }
}