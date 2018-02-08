using Raft.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace Raft.Transport
{
    internal class TcpServer
    {
        public event Action<DateTime, SocketClient, byte[]> OnTcpClientPackageReceived;

        SDictAutoIdentityLong<SocketClient> _socketClients = new SDictAutoIdentityLong<SocketClient>();

        IWarningLog Log = null;
        Socket listener;      
        int Port = 40000;
        bool _isStarted = false;
        bool _isStarting = false;
        object lockObj = new object();
        private object lock_stopServerAsyncStarting = new object();
        private bool _stopServerAsyncStarting = false;

        public TcpServer(IWarningLog log, int port, AddressFamily addressFamily)
        {
            Log = log;          
            Port = port;

            OnTcpClientPackageReceived += TcpServer_OnTcpClientPackageReceived;
        }

        void TcpServer_OnTcpClientPackageReceived(DateTime dt, SocketClient client, byte[] data)
        {
            
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private bool Start()
        {
            //IPAddress[] aryLocalAddr = null;
            //String strHostName = "";
            //IPHostEntry ipEntry;

            try
            {
                if (listener != null)
                    return true;

                //if you want to make it via IPV6->AddressFamily.InterNetworkV6
                listener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                listener.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
                //or IPAddress.IPv6Any
                listener.Bind(new IPEndPoint(IPAddress.Any, this.Port));                
                //backlog is explained here (shortly, queue size for parallel connections, before rejecting connection) http://stackoverflow.com/questions/4253454/question-about-listening-and-backlog-for-sockets                
                listener.Listen((int)SocketOptionName.MaxConnections);

                SocketAsyncEventArgs e = new SocketAsyncEventArgs();
                e.Completed += AcceptCallback;

                if (!listener.AcceptAsync(e))
                    AcceptCallback(listener, e);
                
            }
            catch (System.Exception ex)
            {                
                Log.LogError(new WarningLogEntry() { Exception = ex, Method = "Raft.Transport.TcpServer.Start" });              
                return false;
            }

            
            return true;
        }




        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void AcceptCallback(object sender, SocketAsyncEventArgs e)
        {
            SocketClient client = null;
            Socket skt = null;

            try
            {
                // Socket listener = (Socket)sender;
                if (listener == null)
                {
                    Log.LogError(new WarningLogEntry() { Exception = new Exception("p2_1 restarting listener"), Method = "Raft.Transport.TcpServer.AcceptCallback", Description = "p2_1 restarting listener" });
                    this.Stop();
                    this.Start();
                    return;
                }

                skt = e.AcceptSocket;

                if (skt != null)
                {
                    client = new SocketClient(skt, this);
                    client.SocketAsyncEventArgs = e;
                    client.SocketAsyncEventArgs.DisconnectReuseSocket = true;

                    if (client != null)
                    {
                        client.InstanceId = this._socketClients.Add(client);

                       // onClientConnected(DateTime.Now, client.EndPoint);

                        this.ReceiveData(client);
                    }
                }
                else
                {
                    if (e != null)
                    {
                        e.Dispose();
                        e = null;
                    }
                }
            }
            catch (Exception ex)
            {
                try
                {
                    if (e != null)
                    {
                        e.Dispose();
                        e = null;
                    }
                }
                catch { }

                Log.LogError(new WarningLogEntry() { Exception = ex, Method = "Raft.Transport.TcpServer.AcceptCallback", Description = "p1" });
             
            }


            try
            {
                if (listener == null)
                {
                    Log.LogError(new WarningLogEntry() { Exception = new Exception("p2_2 restarting listener"), Method = "Raft.Transport.TcpServer.AcceptCallback", Description = "p2_2 restarting listener" });
                    
                    this.Stop();
                    this.Start();
                    return;
                }
                else
                {
                    //Creating new e for new Socket

                    SocketAsyncEventArgs e1 = new SocketAsyncEventArgs();
                    e1.Completed += AcceptCallback;

                    if (!listener.AcceptAsync(e1))
                    {
                        AcceptCallback(listener, e1);
                    }
                }
            }
            catch (Exception ex)
            {
                Log.LogError(new WarningLogEntry() { Exception = ex, Method = "Raft.Transport.TcpServer.AcceptCallback", Description = "p2" });                
            }

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="client"></param>
        private void ReceiveData(SocketClient client)
        {
            try
            {
                //Reusing the same object SocketAsyncEventArgs e inr receiving procedures

                client.SocketAsyncEventArgs.Completed -= AcceptCallback;

                client.SocketAsyncEventArgs.SetBuffer(client.Buffer, 0, client.Buffer.Length);

                //client.SocketAsyncEventArgs.Completed += (objSender, objE) => { ReceivedCallback(client); };

                client.SocketAsyncEventArgs.Completed += new EventHandler<SocketAsyncEventArgs>(SocketAsyncEventArgs_Completed);
                client.SocketAsyncEventArgs.UserToken = client;

                if (!client.Socket.ReceiveAsync(client.SocketAsyncEventArgs))
                    ReceivedCallback(client);

            }
            catch (Exception ex)
            {
                Log.LogError(new WarningLogEntry() { Exception = ex, Method = "Raft.Transport.TcpServer.ReceiveData", Description = "p2" });                 
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        void SocketAsyncEventArgs_Completed(object sender, SocketAsyncEventArgs e)
        {
            ReceivedCallback((SocketClient)e.UserToken);
        }

        

        private void ReceivedCallback(SocketClient client)
        {
            //Socket sock = null;

            try
            {
                //sock = (Socket)e.UserToken;
                //sock = ((SocketClient)e.UserToken).Socket;
                var nBytes = client.SocketAsyncEventArgs.BytesTransferred;

                //Console.WriteLine("GOT " + nBytes + " bytes");
                if (nBytes > 0 && client.SocketAsyncEventArgs.SocketError == SocketError.Success)
                {
                    byte[] byReturn = new byte[nBytes];
                    Buffer.BlockCopy(client.Buffer, 0, byReturn, 0, nBytes);

                    //if (!client.IsHandshaked && byReturn[0] == 22)
                    //{
                    //    //SSL, inside this socket will be switched on sync sequential listening inside of YadroSSlStream
                    //    _M.IncomingMessageDispatcher.myTcpServer_onTcpClientPackageReceived(DateTime.Now, client, byReturn);
                    //    //When chrome, gets here, so we go out.
                    //    return;
                    //}
                    //else
                    //{
                    //    onTcpClientPackageReceived(DateTime.Now, client, byReturn);
                    //}

                    OnTcpClientPackageReceived(DateTime.Now, client, byReturn);

                    if (!client.Socket.ReceiveAsync(client.SocketAsyncEventArgs))       //Command ReceiveAsync makes waiting up new info arrives (sometimes info is already arrived and then false will be returned and ReceivedCallback will be called)
                        ReceivedCallback(client);              //Very rare

                    //Going out if all is all right
                    return;
                }
                else
                {
                    //Console.WriteLine("CLIENT REMOVE SOCKET");
                    this.RemoveClient(client);
                    return;
                }


            }
            catch (Exception ex)
            {
                //When socket is terminated, we alsways got here an exception  - it's normal
                //http://stackoverflow.com/questions/2698999/how-to-reuse-socket-in-net                
            }

            this.RemoveClient(client);

        }

        /// <summary>
        /// Removes client from connected list, raises on destroy event to which MUST be subscribed IMD, which makes its operations with the client before disconnect,
        /// then disposes clients of higher level then disposes SocketClient. SocketClient while dispose shutdowns and closes sockets.
        /// </summary>
        /// <param name="client"></param>
        public void RemoveClient(SocketClient client)
        {
            if (client == null)
                return;

            if (client.Disposed)
                return;



            this._socketClients.Remove(client.InstanceId);

            //Big hack to avoid Memory Leaks with SocketAsyncEventArgs
            if (client.SocketAsyncEventArgs.UserToken != null)
            {
                client.SocketAsyncEventArgs.Completed -= new EventHandler<SocketAsyncEventArgs>(SocketAsyncEventArgs_Completed);
                client.SocketAsyncEventArgs.UserToken = null;
            }

            //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! REMARKED
            //onDestroyClient(DateTime.Now, client);
            //onClientDisconnected();
        }


        /// <summary>
        /// 
        /// </summary>
        public void Stop()
        {
            lock (lock_stopServerAsyncStarting)
            {
                _stopServerAsyncStarting = true;
            }

            if (this.listener != null)
            {

                listener.Close();
                listener = null;
            }        

            foreach (var sc in this._socketClients.Values.ToList())
            {
               // onDestroyClient(DateTime.Now, sc);
            }

            this._socketClients.Clear();


            //onClientDisconnected();                                                 //!!!!!!!!!!!!!!!!!!!!!!!!!!!  just makes combobox with client to refresh


            GC.Collect();
            GC.WaitForPendingFinalizers();

            this._isStarted = false;
            this._isStarting = false;

           // onServerStoped();
        }













    }
}
