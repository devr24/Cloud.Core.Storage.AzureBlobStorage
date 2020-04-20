namespace Cloud.Core.Storage.AzureBlobStorage.Models
{
    using System;
    using Microsoft.Azure.Storage.DataMovement;

    /// <summary>
    /// Contains information about a transfer event.
    /// </summary>
    public class TransferEvent :ITransferEvent
    {
        /// <summary>
        /// Easy conversion between classes.
        /// </summary>
        /// <param name="args">Original object type, being implicitly converted from, into TransferEvent type.</param>
        public static implicit operator TransferEvent(TransferEventArgs args)
        {
            return new TransferEvent
            {
                Destination = args.Destination,
                Source = args.Source,
                Exception = args.Exception,
                StartTime = args.StartTime,
                EndTime = args.EndTime
            };
        }
        /// <summary>Gets the instance representation of transfer source location.</summary>
        public object Source { get; set; }

        /// <summary>Gets the instance representation of transfer destination location.</summary>
        public object Destination { get; set; }

        /// <summary>Gets transfer start time.</summary>
        public DateTime StartTime { get; set; }

        /// <summary>Gets transfer end time.</summary>
        public DateTime EndTime { get; set; }

        /// <summary>Gets the exception if the transfer is failed, or null if the transfer is success.</summary>
        public Exception Exception { get;  set; }
    }

    /// <summary>
    /// Type of event that took place.
    /// </summary>
    public enum TransferEventType
    {
        /// <summary>Failed the transfer</summary>
        Failed,
        /// <summary>Tranferred successfully</summary>
        Transferred,
        /// <summary>Skipped the transfer</summary>
        Skipped
    }
}
