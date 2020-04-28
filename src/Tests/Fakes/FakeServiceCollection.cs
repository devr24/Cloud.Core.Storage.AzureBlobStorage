using System.Collections;
using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;

namespace Cloud.Core.Storage.AzureBlobStorage.Tests.Unit
{
    /// <summary>
    /// Class FakeServiceCollection.
    /// Implements the <see cref="Microsoft.Extensions.DependencyInjection.IServiceCollection" />
    /// </summary>
    /// <seealso cref="Microsoft.Extensions.DependencyInjection.IServiceCollection" />
    public class FakeServiceCollection : IServiceCollection
    {
        /// <summary>
        /// The services
        /// </summary>
        List<ServiceDescriptor> _services = new List<ServiceDescriptor>();

        /// <summary>
        /// Gets the enumerator.
        /// </summary>
        /// <returns>IEnumerator&lt;ServiceDescriptor&gt;.</returns>
        public IEnumerator<ServiceDescriptor> GetEnumerator()
        {
            return _services.GetEnumerator();
        }

        /// <summary>
        /// Gets the enumerator.
        /// </summary>
        /// <returns>IEnumerator.</returns>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <summary>
        /// Adds the specified item.
        /// </summary>
        /// <param name="item">The item.</param>
        public void Add(ServiceDescriptor item)
        {
            _services.Add(item);
        }

        /// <summary>
        /// Clears this instance.
        /// </summary>
        public void Clear()
        {
            _services.Clear();
        }

        /// <summary>
        /// Determines whether this instance contains the object.
        /// </summary>
        /// <param name="item">The item.</param>
        /// <returns><c>true</c> if [contains] [the specified item]; otherwise, <c>false</c>.</returns>
        public bool Contains(ServiceDescriptor item)
        {
            return IndexOf(item) != -1;
        }

        /// <summary>
        /// Copies to.
        /// </summary>
        /// <param name="array">The array.</param>
        /// <param name="arrayIndex">Index of the array.</param>
        public void CopyTo(ServiceDescriptor[] array, int arrayIndex)
        {
            _services.CopyTo(array, arrayIndex);
        }

        /// <summary>
        /// Removes the specified item.
        /// </summary>
        /// <param name="item">The item.</param>
        /// <returns><c>true</c> if XXXX, <c>false</c> otherwise.</returns>
        public bool Remove(ServiceDescriptor item)
        {
            return _services.Remove(item);
        }

        /// <summary>
        /// Gets the count.
        /// </summary>
        /// <value>The count.</value>
        public int Count => _services.Count;
        /// <summary>
        /// Gets a value indicating whether this instance is read only.
        /// </summary>
        /// <value><c>true</c> if this instance is read only; otherwise, <c>false</c>.</value>
        public bool IsReadOnly => false;
        /// <summary>
        /// Indexes the of.
        /// </summary>
        /// <param name="item">The item.</param>
        /// <returns>System.Int32.</returns>
        public int IndexOf(ServiceDescriptor item)
        {
            for (int i = 0; i < _services.Count; i++)
            {
                if (_services[i].ServiceType == item?.ServiceType)
                    return i;
            }
            return -1;
        }

        /// <summary>
        /// Inserts the specified index.
        /// </summary>
        /// <param name="index">The index.</param>
        /// <param name="item">The item.</param>
        public void Insert(int index, ServiceDescriptor item)
        {
            _services.Insert(index, item);
        }

        /// <summary>
        /// Removes at.
        /// </summary>
        /// <param name="index">The index.</param>
        public void RemoveAt(int index)
        {
            _services.RemoveAt(index);
        }

        /// <summary>
        /// Gets or sets the <see cref="ServiceDescriptor"/> at the specified index.
        /// </summary>
        /// <param name="index">The index.</param>
        /// <returns>ServiceDescriptor.</returns>
        public ServiceDescriptor this[int index]
        {
            get => _services[index];
            set => _services[index] = value;
        }
    }
}
