using System.Collections.Generic;

namespace Cloud.Core.Storage.AzureBlobStorage.Extensions
{
    internal static class DictionaryExtensions
    {
        /// <summary>
        /// Adds the range.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="Y"></typeparam>
        /// <param name="dictionary">The dictionary to extend.</param>
        /// <param name="items">The items to add.</param>
        /// <returns>IDictionary&lt;T, Y&gt;.</returns>
        internal static IDictionary<T,Y> AddRange<T,Y>(this IDictionary<T,Y> dictionary, IDictionary<T,Y> items)
        {
            if (!items.IsNullOrDefault())
            {
                foreach (var item in items)
                {
                    dictionary.Add(item.Key, item.Value);
                }
            }
            return dictionary;
        }
    }
}
